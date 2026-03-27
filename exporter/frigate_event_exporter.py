#!/usr/bin/env python3
"""
frigate_event_exporter.py

Poll Frigate events and export a 10-second clip per event into a local folder.
No database. Dedup is done by checking for an existing output file per event_id.
Optionally stores a single "last_seen_start_time" marker in a text file to reduce API load.

Designed for Linux + Frigate 0.16.x.

USAGE (examples):
  python3 frigate_event_exporter.py
  FRIGATE_URL="http://127.0.0.1:5000" OUT_DIR="/mnt/frigate/event_clips/clips" python3 frigate_event_exporter.py
  python3 frigate_event_exporter.py --frigate-url http://localhost:5000 --out-dir /mnt/frigate/event_clips/clips --camera reolink_e1_pro --label person

NOTES:
- This script runs ONLY when you start it. To make it automatic, run it as a systemd service or a docker container.
- If you see permission errors (like writing to /data), set OUT_DIR to a path you can write to, e.g. /mnt/frigate/event_clips/clips.
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging
from pathlib import Path

import requests

MAX_CLIPS = 100

def enforce_max_clips(out_dir: Path, max_clips: int = MAX_CLIPS) -> None:
    """
    Keep only the newest `max_clips` files in out_dir (recursively).
    Deletes older files by mtime.
    """
    if not out_dir.exists():
        logging.info("Retention: %s does not exist yet; skipping.", out_dir)
        return

    files = []
    for p in out_dir.rglob("*.mp4"):
        if p.is_file():
            try:
                st = p.stat()
                files.append((st.st_mtime, p))
            except FileNotFoundError:
                continue

    total = len(files)
    if total <= max_clips:
        logging.info("Retention: %d clips present (limit %d). Nothing to delete.", total, max_clips)
        return

    # Newest first
    files.sort(key=lambda x: x[0], reverse=True)

    # Anything beyond max_clips is old and should go
    to_delete = files[max_clips:]
    logging.warning("Retention: %d clips present, deleting %d oldest to keep newest %d.",
                    total, len(to_delete), max_clips)

    deleted = 0
    for _, p in to_delete:
        try:
            p.unlink()
            deleted += 1
            logging.info("Retention: deleted old clip: %s", p)
        except Exception:
            logging.exception("Retention: failed deleting %s", p)

    # Optional: remove now-empty directories (nice housekeeping)
    # Walk bottom-up so children are removed before parents.
    for d in sorted([x for x in out_dir.rglob("*") if x.is_dir()], key=lambda x: len(x.parts), reverse=True):
        try:
            if not any(d.iterdir()):
                d.rmdir()
        except Exception:
            # not fatal; just housekeeping
            pass

    logging.info("Retention: deleted %d old clips; now keeping newest %d.", deleted, max_clips)

# ----------------------------
# Logging
# ----------------------------
def setup_logging(level: str) -> logging.Logger:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("frigate-event-exporter")


# ----------------------------
# Helpers
# ----------------------------
def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def safe_mkdir(path: Path, log: logging.Logger) -> None:
    log.info(f"Ensuring output directory exists: {path}")
    path.mkdir(parents=True, exist_ok=True)


def read_last_seen(marker_file: Path, log: logging.Logger) -> Optional[float]:
    if not marker_file.exists():
        log.info(f"No marker file found at {marker_file} (first run).")
        return None
    try:
        txt = marker_file.read_text().strip()
        if not txt:
            return None
        val = float(txt)
        log.info(f"Loaded last seen start_time marker: {val}")
        return val
    except Exception as e:
        log.warning(f"Could not read marker file {marker_file}: {e}")
        return None


def write_last_seen(marker_file: Path, value: float, log: logging.Logger) -> None:
    try:
        marker_file.write_text(f"{value}\n")
        log.debug(f"Updated marker file {marker_file} => {value}")
    except Exception as e:
        log.warning(f"Could not write marker file {marker_file}: {e}")


def request_json(url: str, timeout: int, log: logging.Logger) -> Any:
    log.debug(f"GET {url}")
    r = requests.get(url, timeout=timeout)
    log.debug(f"Response {r.status_code} from {url}")
    r.raise_for_status()
    return r.json()


def download_stream(url: str, out_path: Path, timeout: int, log: logging.Logger) -> None:
    log.info(f"Downloading: {url}")

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    bytes_written = 0

    with requests.get(url, stream=True, timeout=timeout) as r:
        log.debug(f"Download response status: {r.status_code}")
        r.raise_for_status()

        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
                    bytes_written += len(chunk)

    if bytes_written == 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError("Downloaded 0 bytes (empty clip)")

    tmp_path.replace(out_path)
    log.info(f"Saved clip: {out_path} ({bytes_written} bytes)")

# ----------------------------
# Frigate API logic
# ----------------------------
def fetch_events(
    frigate_url: str,
    limit: int,
    after: Optional[float],
    camera: Optional[str],
    label: Optional[str],
    timeout: int,
    log: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Uses /api/events with optional filters.
    Frigate supports query params like limit, after, camera, label in recent versions.
    If some params are not recognized by your build, you'll still get results and filtering
    is enforced locally as well.
    """
    base = frigate_url.rstrip("/")
    params = [f"limit={limit}"]

    if after is not None:
        # after is seconds since epoch (float ok)
        params.append(f"after={after}")

    if camera:
        params.append(f"camera={camera}")
    if label:
        params.append(f"label={label}")

    url = f"{base}/api/events?{'&'.join(params)}"
    data = request_json(url, timeout=timeout, log=log)

    if not isinstance(data, list):
        log.warning(f"Unexpected events response type: {type(data)}. Response: {data}")
        return []

    log.info(f"Fetched {len(data)} events (limit={limit}, after={after})")
    return data


def event_matches(event: Dict[str, Any], camera: Optional[str], label: Optional[str], log: logging.Logger) -> bool:
    ev_cam = event.get("camera")
    ev_label = event.get("label")

    if camera and ev_cam != camera:
        log.debug(f"Skip {event.get('id')}: camera mismatch (event={ev_cam}, want={camera})")
        return False
    if label and ev_label != label:
        log.debug(f"Skip {event.get('id')}: label mismatch (event={ev_label}, want={label})")
        return False

    # Only process if Frigate says it has a clip (per your JSON examples)
    has_clip = event.get("has_clip")
    if has_clip is False:
        log.debug(f"Skip {event.get('id')}: has_clip=False")
        return False

    return True


def build_export_url(frigate_url: str, camera: str, start_time: float, clip_seconds: int) -> str:
    """
    Prefer the camera time-range clip endpoint:
      /api/<camera>/start/<start>/end/<end>/clip.mp4

    start/end are epoch seconds.

    This yields exactly the time window you want (10 seconds).
    """
    base = frigate_url.rstrip("/")
    start = start_time
    end = start_time + float(clip_seconds)
    return f"{base}/api/{camera}/start/{start}/end/{end}/clip.mp4"


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="Export 10s Frigate event clips to local folder (no DB).")
    parser.add_argument("--frigate-url", default=env("FRIGATE_URL", "http://192.168.50.183:5000"))
    parser.add_argument("--out-dir", default=env("OUT_DIR", "/mnt/frigate/event_clips/clips"))
    parser.add_argument("--camera", default=env("CAMERA_NAME", None))
    parser.add_argument("--label", default=env("LABEL_FILTER", "person"))
    parser.add_argument("--clip-seconds", type=int, default=int(env("CLIP_SECONDS", "10")))
    parser.add_argument("--poll-interval", type=int, default=int(env("POLL_INTERVAL", "15")))
    parser.add_argument("--limit", type=int, default=int(env("EVENT_LIMIT", "100")))
    parser.add_argument("--timeout", type=int, default=int(env("HTTP_TIMEOUT", "15")))
    parser.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    parser.add_argument("--use-marker", action="store_true", default=env("USE_MARKER", "1") == "1")
    args = parser.parse_args()

    log = setup_logging(args.log_level)

    log.info("==== Frigate Event Exporter starting ====")
    log.info(f"Frigate URL      : {args.frigate_url}")
    log.info(f"Output directory : {args.out_dir}")
    log.info(f"Camera filter    : {args.camera or 'ALL'}")
    log.info(f"Label filter     : {args.label or 'ALL'}")
    log.info(f"Clip seconds     : {args.clip_seconds}")
    log.info(f"Poll interval    : {args.poll_interval}s")
    log.info(f"Event limit      : {args.limit}")
    log.info(f"Marker enabled   : {args.use_marker}")

    out_dir = Path(args.out_dir)
    try:
        safe_mkdir(out_dir, log)
    except PermissionError as e:
        log.error(f"Permission error creating output directory {out_dir}: {e}", exc_info=True)
        log.error(
            "Fix: set --out-dir to a writable location. Example:\n"
            "  --out-dir /mnt/frigate/event_clips/clips\n"
            "or ensure permissions:\n"
            "  sudo chown -R $USER:$USER /mnt/frigate/event_clips"
        )
        return 2
    except Exception as e:
        log.error(f"Failed to create output directory {out_dir}: {e}", exc_info=True)
        return 2

    marker_file = out_dir / ".last_seen_start_time"
    last_seen = read_last_seen(marker_file, log) if args.use_marker else None

    # If no marker exists, you can optionally start "recently" by using after=now-3600, etc.
    # To keep it simple, we will not force a backfill. We'll just fetch latest and dedupe by file existence.
    if last_seen is None and args.use_marker:
        log.info("No last_seen marker; will fetch latest events and rely on file-based dedupe.")

    while True:
        try:
            events = fetch_events(
                frigate_url=args.frigate_url,
                limit=args.limit,
                after=(last_seen - 0.001) if last_seen else None
                camera=args.camera,
                label=args.label,
                timeout=args.timeout,
                log=log,
            )

            # Sort oldest->newest by start_time to update marker correctly
            events_sorted = sorted(events, key=lambda e: float(e.get("start_time") or 0.0))

            processed = 0
            downloaded = 0
            newest_start_time = last_seen or 0.0

            for ev in events_sorted:
                ev_id = ev.get("id")
                ev_cam = ev.get("camera")
                ev_label = ev.get("label")
                ev_start = ev.get("start_time")

                log.debug(f"Event: id={ev_id} camera={ev_cam} label={ev_label} start_time={ev_start}")

                if not ev_id or not ev_cam or ev_start is None:
                    log.debug(f"Skipping malformed event: {ev}")
                    continue

                if not event_matches(ev, args.camera, args.label, log):
                    continue

                # Dedup: event_id file exists -> skip
                out_file = out_dir / f"{ev_id}.mp4"
                if out_file.exists():
                    log.info(f"Skipping {ev_id}: already exists ({out_file.name})")
                    newest_start_time = max(newest_start_time, float(ev_start))
                    continue

                # Build 10s export URL
                clip_url = build_export_url(args.frigate_url, ev_cam, float(ev_start), args.clip_seconds)
                log.info(f"Exporting event {ev_id} (camera={ev_cam}, label={ev_label}, start={ev_start})")
                log.debug(f"Clip URL: {clip_url}")

                download_success = False

                for attempt in range(3):
                    try:
                        download_stream(clip_url, out_file, timeout=args.timeout, log=log)
                        downloaded += 1
                        download_success = True
                        break
                    except Exception as e:
                        log.warning(f"Download attempt {attempt+1}/3 failed for {ev_id}: {e}")
                        if attempt == 2:
                            raise
                        time.sleep(1)
                except Exception as e:
                    log.error(f"Failed to export {ev_id}: {e}", exc_info=True)
                    # Cleanup partial
                    part = out_file.with_suffix(out_file.suffix + ".part")
                    if part.exists():
                        try:
                            part.unlink()
                        except Exception:
                            pass

                newest_start_time = max(newest_start_time, float(ev_start))
                processed += 1

            enforce_max_clips(out_dir, MAX_CLIPS)

            if args.use_marker and newest_start_time and newest_start_time != (last_seen or 0.0):
                last_seen = newest_start_time
                write_last_seen(marker_file, last_seen, log)

            log.info(
                f"Poll cycle complete. events={len(events)} processed={processed} downloaded={downloaded} "
                f"last_seen={last_seen if args.use_marker else 'DISABLED'}"
            )

        except requests.ConnectionError as e:
            log.error(f"Cannot connect to Frigate at {args.frigate_url}: {e}", exc_info=True)
        except requests.Timeout as e:
            log.error(f"Timed out talking to Frigate: {e}", exc_info=True)
        except Exception as e:
            log.error(f"Unexpected error in poll cycle: {e}", exc_info=True)

        log.info(f"Sleeping {args.poll_interval}s...")
        time.sleep(args.poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
