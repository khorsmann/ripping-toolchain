# Ripping Toolchain

This project automates my personal workflow for digitizing my own DVDs. The scripts are for private use only (German "Privatkopie" right) and must not be used to create or distribute unlicensed copies.

> ⚠️ **Note:** This is heavily "vibed-coded" hobbyware and not production-hardened. If the Flying Spaghetti Monster shakes the cables, things may break at any time—use at your own risk.


## Architecture Overview

```
DVD ----> ripper (MakeMKV) --raw MKVs--> /media/raw/(dvd|bluray)
                         |
                         +--> MQTT event media/rip/done

transcode_mqtt (FFmpeg) <--- MQTT subscription
        |
        +--(media/transcode/start|done|error)--> MQTT status
        |
        +--> /media/Serien (HEVC transcodes)
```

- **ripper/ripper.py** inspects an inserted DVD via the MakeMKV CLI, picks suitable episodes based on a chapter-duration heuristic, rips them to `base_raw/<source_type>`, then publishes an MQTT event (`media/rip/done`) that contains path, series, season, disc, etc.
- **transcode/transcode_mqtt.py** runs as a service, subscribes to the MQTT topic `media/rip/done`, queues each incoming path event, and transcodes every `.mkv` inside to `SERIES_DST_BASE` (default `/media/Serien`) or `MOVIE_DST_BASE` via `ffmpeg` + VAAPI hardware acceleration. Progress and errors are published on `media/transcode/start`, `media/transcode/done`, or `media/transcode/error`.
- Optional integrations (e.g., Home Assistant) can react to both rip and transcode topics, see `misc/homeassistant/`.
- **transcode/rescan.py** compares the raw tree (`SRC_BASE`) with the targets (`SERIES_DST_BASE`/`MOVIE_DST_BASE`) and sends MQTT jobs for every source directory missing transcoded MKVs; `--dry-run` only shows what would be sent. It can load the same env file as the service (`--env-file`, default `/etc/transcode-mqtt.env`).


## Components & Flow

1. **Start the ripper**  
   ```
   ./ripper/ripper.py \
     --series Your_cool_series_title \
     --season 02 \
     --disc disc07 \
     --episode-start 23 \
     --config ripper/ripper.toml
   ```
  - Reads `ripper.toml` (MQTT, DVD drive, storage, heuristic) and checks MQTT connectivity first.
  - The heuristic allows a minimum (`min_episode_minutes`) and optional maximum (`max_episode_minutes`) runtime so "whole-disc titles" (e.g., "title 0" with all episodes) can be ignored; `--movie-name <title>` switches to movie mode where only the minimum length applies.
  - In movie mode, `--series`, `--season`, `--disc`, and `--episode-start` are omitted; the file is stored as `<base_raw>/<source_type>/<movie_path>/<Title>.mkv` (plus info file). `movie_path` comes from the storage config (default `Filme`); `<Title>` is the provided (normalized) `--movie-name`.
  - Invokes `makemkvcon` (`info` and `mkv`), renames the produced files, and stores series under `<base_raw>/<source_type>/<series_path>/<Series>/S<Season>/<Disc>` (default `series_path = "Serien"`).
  - Publishes `version = 1` in every `media/rip/done` payload; transcode-mqtt strictly rejects other versions.
   - Ejects the drive at the end and publishes the MQTT payload mentioned above.

2. **Transcode service**  
   - Typically runs via systemd (`transcode/transcode-mqtt.service`) and loads its environment from `/etc/transcode-mqtt.env`.
   - When a `media/rip/done` event arrives, the path is placed in an internal queue. A worker thread processes the directory sequentially:
     - Before each file, it publishes `media/transcode/start` including input and output paths.
     - While `ffmpeg` runs, a lock at `/var/lock/vaapi.lock` keeps other instances off the GPU.
     - Hardware retries are configurable via `MAX_HW_RETRIES` (default 2 after the initial attempt).
     - After a successful transcode, it publishes `media/transcode/done`; failures land on `media/transcode/error`.
   - Idempotent: if the target file already exists, it is skipped.
   - Series go to `SERIES_DST_BASE` (default `/media/Serien`) mirroring the structure under `SRC_BASE/<SERIES_SUBPATH>` (default `Serien`). Movies (`mode=movie`) are stored under `MOVIE_DST_BASE` (default `/media/Filme`, overridable).
   - All status payloads (`media/transcode/*`) also contain `version = 1` to stay aligned with the same protocol.
   - If a transcoded file is missing later, `transcode/rescan.py` can rescan the raw tree and send MQTT jobs for the missing targets (`--dry-run` to inspect).


## Dependencies

### Common
- Linux system with sufficient space at `/media/raw` and `/media/Serien` (or customized paths).
- Running **Mosquitto** broker (or compatible) with user/password for both the ripper and transcode service.
- Python >= 3.11 plus modules:
  - `paho-mqtt`
  - `tomllib` (built in starting with 3.11; otherwise `tomli`)

### Ripper-specific
- **MakeMKV CLI** (`makemkvcon`) with a valid license/key.
- Access to the DVD drive (e.g., `/dev/sr0`) and the `eject` command.
- Optional: shell tools like `mosquitto_pub` for manual testing (`test/` scripts).

### Transcode-specific
- **FFmpeg** with VAAPI support and access to the GPU (`/dev/dri/renderD128`).
- Write permissions for `/var/lock` (for `vaapi.lock`) and the configured destination storage.
- Optional log rotation for `/var/log/transcode-mqtt.log` when run via the systemd unit.


## Operational Notes

- Both components assume `series_path` (ripper) and `SERIES_SUBPATH` (transcode) match. If `SRC_BASE` has no subfolders, series live under `SRC_BASE/<SERIES_SUBPATH>`; if `SRC_BASE` contains `dvd/` or `bluray/`, the layout lives under `SRC_BASE/<source_type>/<SERIES_SUBPATH>`. Movies are exempt and live together under `MOVIE_DST_BASE`.
- MQTT topics can be adjusted via environment variables; defaults are `media/rip/done` for inputs and `media/transcode/*` for status.
- The workflow exists solely to digitize personally purchased media for private use. Respect third-party rights (DRM, copyright); sharing or publicly providing ripped/transcoded files is not intended.
