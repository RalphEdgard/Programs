#!/usr/bin/env python3
from __future__ import annotations

import os
import time
import subprocess
from typing import Optional

DROPBOX_SCRIPT = "/mnt/frigate/event_clips/DropBoxToggle.py"
POLL_SECONDS = int(os.getenv("OTA_POLL_SECONDS", "15"))


def run_dropbox_toggle() -> str:
    p = subprocess.run(
        ["/usr/bin/python3", DROPBOX_SCRIPT],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    out = (p.stdout or "")
    err = (p.stderr or "")
    return (out + (("\n" + err) if err else "")).strip()


def decide_action(output: str) -> Optional[str]:
    s = output.lower()
    if "deactivate" in s:
        return "deactivate"
    if "activate" in s:
        return "activate"
    return None


def extract_modified_epoch(output: str) -> Optional[int]:
    from datetime import datetime, timezone

    for line in output.splitlines():
        line = line.strip()
        if line.lower().startswith("modified:"):
            ts = line.split(":", 1)[1].strip()

            try:
                # If Dropbox gives a timezone (Z or +00:00), fromisoformat handles it (after minor normalization).
                ts_norm = ts.replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts_norm)

                # If it's "naive" (no tzinfo), assume UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                return int(dt.timestamp())

            except Exception:
                return None
    return None

def write_control(uploads_enabled: bool, start_after_epoch: int | None = None) -> None:
    import json
    import tempfile

    control_file = os.getenv("CONTROL_FILE", "/mnt/frigate/event_clips/gdrive_json/uploader_control.json")

    payload = {"uploads_enabled": uploads_enabled}
    if start_after_epoch is not None and start_after_epoch > 0:
        payload["start_after_epoch"] = int(start_after_epoch)

    d = os.path.dirname(control_file)
    os.makedirs(d, exist_ok=True)

    fd, tmp = tempfile.mkstemp(prefix="uploader_control_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, control_file)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


if __name__ == "__main__":
    last_action: Optional[str] = None
    last_epoch: Optional[int] = None

    while True:
        output = run_dropbox_toggle()
        action = decide_action(output)

        if action == "deactivate":
            if last_action != "deactivate":
                print("Home/Deactivate -> pausing uploads (control file)")
            write_control(False)
            last_action = "deactivate"
            last_epoch = None

        elif action == "activate":
            epoch = extract_modified_epoch(output)
            if epoch is None:
                # fallback: enable uploads without cutoff
                if last_action != "activate":
                    print("Home/Activate -> enabling uploads (no cutoff found)")
                write_control(True)
            else:
                if last_action != "activate" or last_epoch != epoch:
                    print(f"Home/Activate -> enabling uploads start_after_epoch={epoch}")
                write_control(True, start_after_epoch=epoch)
                last_epoch = epoch

            last_action = "activate"

        time.sleep(POLL_SECONDS)
