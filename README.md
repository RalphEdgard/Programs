# Frigate Event Processing & Google Drive Upload Pipeline

## Overview

This project implements an automated pipeline for processing motion events detected by Frigate, generating video clips, and uploading them to Google Drive.

The system is designed to run continuously on a Linux server and consists of multiple cooperating components that handle:

- Event detection ingestion
- Clip generation from recordings
- Upload management
- External control (OTA-style enable/disable)

---

## Architecture

```
Frigate → PNG snapshots / recordings
               ↓
        gdrive_uploader.py
               ↓
        clip_builder.py (ffmpeg processing)
               ↓
        Google Drive

Control Plane:
ota_toggle_daemon.py → dynamically enables/disables uploads
```

---

## Project Structure

```
exporter/
    frigate_event_exporter.py   # Pulls clips directly from Frigate API

gdrive/
    gdrive_uploader.py          # Core uploader + processing pipeline
    clip_builder.py             # Segment stitching + ffmpeg logic
    ota_toggle_daemon.py        # Control loop (enable/disable uploads)
```

---

## Component Breakdown

### `gdrive_uploader.py`
- Scans for Frigate snapshot images (`*-clean.png`)
- Deduplicates uploads using file metadata (no database required)
- Builds clips from raw recordings OR uploads segments
- Uploads files to Google Drive using OAuth
- Supports dynamic runtime control via a JSON control file

### `clip_builder.py`
- Parses event timestamps from snapshot filenames
- Locates relevant video segments from Frigate recordings
- Concatenates segments using ffmpeg
- Optionally trims clips to precise time windows

### `ota_toggle_daemon.py`
- Polls an external script (e.g., Dropbox toggle)
- Dynamically enables or disables uploads
- Writes control state to a shared JSON file

### `frigate_event_exporter.py`
- Optional component
- Pulls clips directly from Frigate API instead of building from segments
- Includes retention logic to limit disk usage

---

## Features

- No database required (stateless deduplication via JSONL)
- Deterministic file tracking using path + size + mtime
- Supports both full clip generation (ffmpeg) and direct segment uploads
- Runtime control without restarting services
- Designed for long-running Linux environments

---

## Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

### Python Dependencies
- `google-api-python-client`
- `google-auth`
- `google-auth-oauthlib`
- `google-auth-httplib2`
- `requests`

### System Dependencies
- `ffmpeg` (required for clip generation)

---

## Environment Variables

```env
SCAN_DIR=/mnt/frigate/clips
RECORDINGS_DIR=/mnt/frigate/recordings
CLIP_OUT_DIR=/mnt/frigate/event_clips/generated_clips

GOOGLE_CREDENTIALS=/etc/frigate-tools/credentials.json
GOOGLE_TOKEN=/etc/frigate-tools/token.json

CONTROL_FILE=/etc/frigate-tools/uploader_control.json
```

---

## Security Notes

The following files are **not** committed to Git:
- `credentials.json` (OAuth client)
- `token.json` (OAuth token)
- Generated clips / recordings

Ensure proper file permissions are set on sensitive paths.

---

## Deployment

Recommended layout:

```
/opt/frigate-tools/        # code (this repository)
/mnt/frigate/              # data (recordings, clips)
```

Run scripts from:

```bash
cd /opt/frigate-tools
python3 gdrive/gdrive_uploader.py
```

---

## AI Assistance Disclosure

This project was developed with the assistance of AI tools.

AI was used to:
- Accelerate development of boilerplate and integrations
- Assist with debugging and refactoring
- Explore architectural approaches for the pipeline

However:
- The system design, structure, and integration decisions were implemented and validated by the author
- The author understands the data flow between components, environment configuration and runtime behavior, the Linux execution model and service design, and the interaction between Frigate, file storage, and Google Drive APIs

AI was used as a tool for productivity, not as a replacement for understanding.

---

## Design Philosophy

- Prefer stateless systems over database-backed designs
- Keep components modular and composable
- Optimize for long-running reliability
- Avoid unnecessary dependencies
- Maintain clear separation between the data plane (processing) and control plane (OTA toggle)

---

## Future Improvements

- Systemd service definitions for all components
- Retry/backoff strategies for network operations
- Parallel clip processing
- Monitoring and alerting
- Containerization (Docker)

---

## License

MIT
