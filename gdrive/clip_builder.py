from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

PNG_TS_RE = re.compile(r"^(?P<cam>.+?)-(?P<epoch>\d+(?:\.\d+)?)-.+-clean\.png$")

@dataclass
class ClipSpec:
    camera: str
    event_ts: float
    start_ts: float
    end_ts: float
    date_str: str
    hour_str: str

def parse_event_from_png(png_path: Path) -> Optional[Tuple[str, float]]:
    m = PNG_TS_RE.match(png_path.name)
    if not m:
        return None
    cam = m.group("cam")
    epoch = float(m.group("epoch"))
    return cam, epoch

def build_clip_spec(camera: str, event_ts: float, pre_sec: int, post_sec: int) -> ClipSpec:
    start_ts = event_ts - pre_sec
    end_ts = event_ts + post_sec

    # Frigate recordings folders here appear to be UTC-based (your test: 08:11 local -> hour 13 folder)
    dt_utc = datetime.fromtimestamp(event_ts, tz=timezone.utc)
    date_str = dt_utc.strftime("%Y-%m-%d")
    hour_str = dt_utc.strftime("%H")

    return ClipSpec(
        camera=camera,
        event_ts=event_ts,
        start_ts=start_ts,
        end_ts=end_ts,
        date_str=date_str,
        hour_str=hour_str,
    )

def recordings_dir(base_recordings: Path, spec: ClipSpec) -> Path:
    return base_recordings / spec.date_str / spec.hour_str / spec.camera

def iter_segments_for_window(seg_dir: Path, spec: ClipSpec) -> List[Path]:
    if not seg_dir.exists():
        return []

    chosen: List[Tuple[float, Path]] = []

    # Anchor to UTC hour boundary (must match your folder selection)
    hour_start = (
        datetime.fromtimestamp(spec.event_ts, tz=timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
        .timestamp()
    )

    for p in seg_dir.glob("*.mp4"):
        name = p.stem  # "00.06"
        if "." not in name:
            continue
        mm_s, ss_s = name.split(".", 1)
        if not (mm_s.isdigit() and ss_s.isdigit()):
            continue

        mm = int(mm_s)
        ss = int(ss_s)

        seg_start = hour_start + (mm * 60) + ss
        seg_end = seg_start + 10.0  # assume 10s segments

        if seg_end >= spec.start_ts and seg_start <= spec.end_ts:
            chosen.append((seg_start, p))

    chosen.sort(key=lambda t: t[0])
    return [p for _, p in chosen]

def make_concat_file(paths: List[Path], concat_path: Path) -> None:
    concat_path.parent.mkdir(parents=True, exist_ok=True)
    with concat_path.open("w", encoding="utf-8") as f:
        for p in paths:
            # ffmpeg concat demuxer requires "file '/path'"
            f.write(f"file '{str(p)}'\n")

def render_clip(
    seg_paths: List[Path],
    out_mp4: Path,
    spec: ClipSpec,
    strict_trim: bool = True,
) -> None:
    """
    Concatenate segments and optionally trim to exact window.
    """

    out_mp4.parent.mkdir(parents=True, exist_ok=True)

    concat_txt = out_mp4.with_suffix(".concat.txt")
    make_concat_file(seg_paths, concat_txt)

    tmp_cat = out_mp4.with_suffix(".concat.mp4")

    # 1) concat (fast, stream copy)
    cmd_concat = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_txt),
        "-c", "copy",
        str(tmp_cat),
    ]
    subprocess.run(cmd_concat, check=True, capture_output=True, text=True)

    if not strict_trim:
        os.replace(tmp_cat, out_mp4)
        return

    # 2) trim to exact window relative to tmp_cat timeline:
    # compute offset: how far into tmp_cat the window start is.
    # tmp_cat starts at the first segment start time.
    first_start = None
    for p in seg_paths:
        st = p.stem
        mm, ss = st.split(".", 1)
        
        hour_start = (
            datetime.fromtimestamp(spec.event_ts, tz=timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
            .timestamp()
        )
        first_start = hour_start + (int(mm) * 60) + int(ss)
        break

    if first_start is None:
        os.replace(tmp_cat, out_mp4)
        return

    ss_offset = max(0.0, spec.start_ts - first_start)
    duration = max(1.0, spec.end_ts - spec.start_ts)

    cmd_trim = [
        "ffmpeg", "-y",
        "-ss", f"{ss_offset:.3f}",
        "-i", str(tmp_cat),
        "-t", f"{duration:.3f}",
        "-c", "copy",
        str(out_mp4),
    ]
    subprocess.run(cmd_trim, check=True, capture_output=True, text=True)

    try:
        tmp_cat.unlink(missing_ok=True)
        concat_txt.unlink(missing_ok=True)
    except Exception:
        pass
