#!/usr/bin/env python3
"""
gdrive_uploader.py

Upload Frigate snapshots (*-clean.png) to Google Drive without uploading duplicates.
No database: dedupe uses a local JSONL log + deterministic key (path+size+mtime).

Default scan dir: /mnt/frigate/clips
Default pattern : *-clean.png

Auth:
- Uses OAuth "Installed app" credentials.json (Desktop app).
- First run creates token.json.
- For headless servers: use --auth-console to print a URL you open elsewhere and paste back the code.
  If Google blocks console flow for your org/account, do first auth on a machine with a browser and
  copy token.json to the server.

Requires:
  pip install --upgrade google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from clip_builder import parse_event_from_png, build_clip_spec, recordings_dir, iter_segments_for_window, render_clip
import mimetypes

# Scope: "app-created files only" (recommended). Files uploaded by this script are manageable by it.
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

@dataclass
class Config:
    scan_dir: Path
    pattern: str
    credentials_path: Path
    token_path: Path
    uploaded_log: Path
    poll_seconds: int
    run_once: bool
    drive_folder_id: Optional[str]
    log_level: str
    start_after_epoch: Optional[int]
    control_file: Optional[Path]
    recordings_dir: Path
    clip_pre_seconds: int
    clip_post_seconds: int
    clip_out_dir: Path
    upload_clips: bool
    upload_segments: bool
    segments_post_seconds: int

def sort_key(p: Path):
    st = p.stat()
    return (st.st_mtime_ns, st.st_size, str(p.resolve()))

def setup_logging(level: str) -> logging.Logger:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("gdrive-uploader")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def file_key_from_stat(path: Path, st) -> str:
    basis = f"{str(path.resolve())}|{st.st_size}|{st.st_mtime_ns}"
    return sha1(basis)

def load_uploaded_set(uploaded_log: Path, log: logging.Logger) -> Dict[str, Dict]:
    """
    Load uploaded keys from JSONL log.
    Keyed by file_key.
    """
    uploaded: Dict[str, Dict] = {}
    if not uploaded_log.exists():
        log.info("No upload log found at %s (first run).", uploaded_log)
        return uploaded

    try:
        with uploaded_log.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    k = rec.get("key")
                    if k:
                        uploaded[k] = rec
                except json.JSONDecodeError:
                    continue
        log.info("Loaded %d uploaded entries from %s", len(uploaded), uploaded_log)
    except Exception:
        log.exception("Failed reading uploaded log: %s", uploaded_log)

    return uploaded


def append_uploaded(uploaded_log: Path, rec: Dict, log: logging.Logger) -> None:
    uploaded_log.parent.mkdir(parents=True, exist_ok=True)
    with uploaded_log.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    log.debug("Logged upload: %s", rec.get("name"))


def get_drive_service(cfg: Config, log: logging.Logger):
    """
    Build Drive API service with OAuth credentials.
    """
    if not cfg.credentials_path.exists():
        raise FileNotFoundError(
            f"Missing credentials.json at {cfg.credentials_path}. "
            f"Download OAuth client JSON (Desktop app) and put it there."
        )

    creds = None
    if cfg.token_path.exists():
        creds = Credentials.from_authorized_user_file(str(cfg.token_path), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        log.info("Refreshing expired token...")
        creds.refresh(Request())
    elif not creds or not creds.valid:
        log.error(
            "No valid token found. This server is headless.\n"
            "Authenticate ONCE on a machine with a browser, then copy token.json here."
        )
        raise RuntimeError("Missing or invalid OAuth token")

    cfg.token_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.token_path.write_text(creds.to_json())
    log.info("Saved token to %s", cfg.token_path)

    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service


def iter_files(scan_dir: Path, pattern: str) -> Iterable[Path]:
    # recursive glob
    yield from scan_dir.rglob(pattern)


def ensure_folder_exists(service, folder_name: str, log: logging.Logger) -> str:
    """
    Create (or reuse) a folder in Drive root. Returns folder_id.
    Only used if user doesn't provide --drive-folder-id.
    """
    # Look for existing folder
    q = (
        f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false"
    )
    res = service.files().list(q=q, spaces="drive", fields="files(id,name)").execute()
    files = res.get("files", [])
    if files:
        folder_id = files[0]["id"]
        log.info("Using existing Drive folder '%s' (id=%s)", folder_name, folder_id)
        return folder_id

    meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}
    created = service.files().create(body=meta, fields="id").execute()
    folder_id = created["id"]
    log.info("Created Drive folder '%s' (id=%s)", folder_name, folder_id)
    return folder_id


def upload_file(service, local_path: Path, parent_folder_id: Optional[str], log: logging.Logger) -> Tuple[str, str]:
    file_metadata = {"name": local_path.name}
    if parent_folder_id:
        file_metadata["parents"] = [parent_folder_id]

    mime, _ = mimetypes.guess_type(str(local_path))
    if not mime:
        mime = "application/octet-stream"

    size = local_path.stat().st_size
    resumable = size >= 25 * 1024 * 1024  # resumable only for 25MB+
    media = MediaFileUpload(str(local_path), mimetype=mime, resumable=resumable)

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink",
    ).execute()

    file_id = created["id"]
    link = created.get("webViewLink", "")
    log.info("Uploaded %s (%s) -> Drive file id=%s", local_path.name, mime, file_id)
    return file_id, link

def load_control(cfg: Config, log: logging.Logger) -> Dict:
    if not cfg.control_file:
        return {}

    try:
        if not cfg.control_file.exists():
            return {}
        
        log.debug("Reading control file: %s", cfg.control_file)
        raw = cfg.control_file.read_text(encoding="utf-8").strip()
        if not raw:
            return {}

        return json.loads(raw)

    except Exception as e:
        log.warning(
            "Failed to read control file %s: %s",
            cfg.control_file,
            e,
        )
        return {}

def run_once(cfg: Config, service, uploaded: Dict[str, Dict], log: logging.Logger) -> int:
    if not cfg.scan_dir.exists():
        log.error("Scan directory does not exist: %s", cfg.scan_dir)
        return 2


    control = load_control(cfg, log)
    log.info("CONTROL_FILE resolved to: %s", cfg.control_file)
    uploads_enabled = control.get("uploads_enabled", True)

    if not uploads_enabled:
        log.info("Uploads paused by control file: %s", cfg.control_file)
        return 0

    # Optional dynamic cutoff override from control file
    ctrl_cutoff = control.get("start_after_epoch")
    if isinstance(ctrl_cutoff, (int, float)) and ctrl_cutoff > 0:
        cfg.start_after_epoch = int(ctrl_cutoff)


    parent_folder_id = cfg.drive_folder_id
    if not parent_folder_id:
        parent_folder_id = ensure_folder_exists(service, "Frigate Clean Snapshots", log)

    raw_candidates = list(iter_files(cfg.scan_dir, cfg.pattern))

    uploaded_count = 0
    skipped = 0
    failed = 0

    # Build deterministic list
    safe_candidates = []
    for p in raw_candidates:
        try:
            st0 = p.stat()
            safe_candidates.append((st0, str(p.resolve()), p))
        except FileNotFoundError:
            continue

    safe_candidates.sort(key=lambda t: (t[0].st_mtime_ns, t[0].st_size, t[1]))
    candidates = [(t[2], t[0]) for t in safe_candidates]

    # NEW: compute cutoff ONCE, before iterating
    cutoff_ns = None
    if cfg.start_after_epoch:
        cutoff_ns = int(cfg.start_after_epoch) * 1_000_000_000

    for p, st in candidates:
        try:
            if not p.is_file():
                continue

            # NEW: cutoff check uses st
            if cutoff_ns and st.st_mtime_ns < cutoff_ns:
                skipped += 1
                log.debug(
                    "Skip (older than cutoff): %s mtime_ns=%s cutoff_ns=%s",
                    p, st.st_mtime_ns, cutoff_ns
                )
                continue

            k = file_key_from_stat(p, st)
            if k in uploaded:
                skipped += 1
                log.debug("Skip (already uploaded): %s", p)
                continue

            file_id, link = upload_file(service, p, parent_folder_id, log)
            time.sleep(0.5)
            
            rec = {
                "key": k,
                "path": str(p.resolve()),
                "name": p.name,
                "size": st.st_size,
                "mtime_ns": st.st_mtime_ns,
                "drive_file_id": file_id,
                "drive_folder_id": parent_folder_id,
                "uploaded_at": time.time(),
                "webViewLink": link,
            }
            append_uploaded(cfg.uploaded_log, rec, log)

            if cfg.upload_clips:
                parsed = parse_event_from_png(p)
                if not parsed:
                    log.warning("Could not parse event time from png filename: %s", p.name)
                else:
                    cam, event_ts = parsed
                    spec = build_clip_spec(cam, event_ts, cfg.clip_pre_seconds, cfg.clip_post_seconds)

                    seg_dir = recordings_dir(cfg.recordings_dir, spec)
                    segs = iter_segments_for_window(seg_dir, spec)

                    log.info("Clip lookup: cam=%s event=%s seg_dir=%s segments=%d",
                            cam, datetime.fromtimestamp(event_ts).isoformat(), seg_dir, len(segs))

                    if segs:
                        # FAST PATH: upload segments (chunks) immediately (no ffmpeg)
                        if cfg.upload_segments:
                            # Make a spec for an "immediate" window: do NOT wait for post segments.
                            # We cap post to cfg.segments_post_seconds (default 0 = immediate).
                            immediate_spec = build_clip_spec(cam, event_ts, cfg.clip_pre_seconds, cfg.segments_post_seconds)

                            seg_dir2 = recordings_dir(cfg.recordings_dir, immediate_spec)
                            segs2 = iter_segments_for_window(seg_dir2, immediate_spec)

                            log.info(
                                "Immediate segment lookup: cam=%s event=%s seg_dir=%s segments=%d pre=%ds post=%ds",
                                cam,
                                datetime.fromtimestamp(event_ts).isoformat(),
                                seg_dir2,
                                len(segs2),
                                cfg.clip_pre_seconds,
                                cfg.segments_post_seconds,
                            )

                            for seg in segs2:
                                try:
                                    # Name in Drive will still be seg.name, but this log helps you track
                                    log.info("Uploading segment: %s", seg)
                                    file_id_s, link_s = upload_file(service, seg, parent_folder_id, log)
                                    time.sleep(0.5)
                                    log.info("Uploaded segment %s -> %s", seg.name, file_id_s)
                                except Exception as e:
                                    log.exception("Failed uploading segment %s for event png %s: %s", seg, p.name, e)

                        else:
                            # SLOW PATH: build one stitched clip (existing behavior)
                            out_name = f"{cam}-{event_ts:.3f}-clip.mp4"
                            out_mp4 = cfg.clip_out_dir / spec.date_str / spec.hour_str / cam / out_name
                            try:
                                render_clip(segs, out_mp4, spec, strict_trim=True)
                                file_id2, link2 = upload_file(service, out_mp4, parent_folder_id, log)
                                time.sleep(0.5)
                                log.info("Uploaded clip %s -> %s", out_mp4.name, file_id2)
                            except Exception as e:
                                log.exception("Failed building/uploading clip for %s: %s", p.name, e)
                    else:
                        log.warning("No segments found for clip window. event=%s seg_dir=%s",
                            datetime.fromtimestamp(event_ts).isoformat(), seg_dir)


            uploaded[k] = rec
            uploaded_count += 1

        except Exception as e:
            failed += 1
            log.exception("Failed uploading %s: %s", p, e)

    log.info("Done. uploaded=%d skipped=%d failed=%d", uploaded_count, skipped, failed)
    return 0 if failed == 0 else 3


def parse_args() -> Config:
    def env(name: str, default: Optional[str] = None) -> Optional[str]:
        v = os.getenv(name)
        return v if v not in (None, "") else default

    parser = argparse.ArgumentParser(
        description="Upload Frigate *-clean.png snapshots to Google Drive (dedupe, no DB)."
    )

    parser.add_argument("--scan-dir", default=env("SCAN_DIR", "/mnt/frigate/clips"))
    parser.add_argument("--pattern", default=env("PATTERN", "*-clean.png"))

    parser.add_argument("--credentials",
        default=env("GOOGLE_CREDENTIALS", "/etc/frigate-tools/credentials.json"))

    parser.add_argument("--token",
        default=env("GOOGLE_TOKEN", "/etc/frigate-tools/token.json"))

    parser.add_argument("--uploaded-log",
        default=env("UPLOADED_LOG", "/mnt/frigate/event_clips/uploaded_clean_png.jsonl"))

    parser.add_argument("--control-file",
        default=env("CONTROL_FILE", "/etc/frigate-tools/uploader_control.json"))

    parser.add_argument("--poll-seconds", type=int, default=int(env("POLL_SECONDS", "30")))
    parser.add_argument("--run-once", action="store_true", default=env("RUN_ONCE", "0") == "1")
    parser.add_argument("--drive-folder-id", default=env("DRIVE_FOLDER_ID", None))
    parser.add_argument("--auth-console", action="store_true", default=env("AUTH_CONSOLE", "0") == "1")
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))

    parser.add_argument("--recordings-dir", default=env("RECORDINGS_DIR", "/mnt/frigate/recordings"))
    parser.add_argument("--clip-pre-seconds", type=int, default=int(env("CLIP_PRE_SECONDS", "10")))
    parser.add_argument("--clip-post-seconds", type=int, default=int(env("CLIP_POST_SECONDS", "10")))
    parser.add_argument("--clip-out-dir", default=env("CLIP_OUT_DIR", "/mnt/frigate/event_clips/generated_clips"))

    parser.add_argument("--upload-clips", action="store_true", default=env("UPLOAD_CLIPS", "1") == "1")
    parser.add_argument("--upload-segments", action="store_true", default=env("UPLOAD_SEGMENTS", "0") == "1")
    parser.add_argument("--segments-post-seconds", type=int, default=int(env("SEGMENTS_POST_SECONDS", "0")))

    # New: cutoff from env/cli
    parser.add_argument("--start-after-epoch", type=int, default=int(env("START_AFTER_EPOCH", "0")))

    args = parser.parse_args()

    return Config(
        scan_dir=Path(args.scan_dir),
        pattern=args.pattern,
        credentials_path=Path(args.credentials),
        token_path=Path(args.token),
        uploaded_log=Path(args.uploaded_log),
        poll_seconds=args.poll_seconds,
        run_once=args.run_once,
        drive_folder_id=args.drive_folder_id,
        log_level=args.log_level,
        start_after_epoch=args.start_after_epoch if args.start_after_epoch > 0 else None,
        control_file=Path(args.control_file) if args.control_file else None,

        recordings_dir=Path(args.recordings_dir),
        clip_pre_seconds=args.clip_pre_seconds,
        clip_post_seconds=args.clip_post_seconds,
        clip_out_dir=Path(args.clip_out_dir),
        upload_clips=bool(args.upload_clips),
        upload_segments=bool(args.upload_segments),
        segments_post_seconds=int(args.segments_post_seconds),    
    )

def main() -> int:
    cfg = parse_args()
    log = setup_logging(cfg.log_level)

    log.info("==== Google Drive Uploader starting ====")
    log.info("Scan dir        : %s", cfg.scan_dir)
    log.info("Pattern         : %s", cfg.pattern)
    log.info("Credentials     : %s", cfg.credentials_path)
    log.info("Token           : %s", cfg.token_path)
    log.info("Uploaded log    : %s", cfg.uploaded_log)
    log.info("Poll seconds    : %s", cfg.poll_seconds)
    log.info("Run once        : %s", cfg.run_once)
    log.info("Drive folder id : %s", cfg.drive_folder_id or "(auto-create 'Frigate Clean Snapshots')")

    uploaded = load_uploaded_set(cfg.uploaded_log, log)

    try:
        service = get_drive_service(cfg, log)
    except Exception as e:
        log.exception("Auth / Drive service setup failed: %s", e)
        return 2

    if cfg.run_once:
        return run_once(cfg, service, uploaded, log)

    while True:
        rc = run_once(cfg, service, uploaded, log)
        if rc not in (0, 3):  # 3 means some uploads failed; keep running
            log.warning("Cycle returned rc=%d", rc)
        log.info("Sleeping %ds...", cfg.poll_seconds)
        time.sleep(cfg.poll_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
