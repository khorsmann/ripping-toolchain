#!/usr/bin/env python3
"""
Vergleicht die Raw-Struktur (SRC_BASE) mit dem transkodierten Ziel (SERIES_DST_BASE/MOVIE_DST_BASE)
und publiziert MQTT-Jobs für alle Quell-Verzeichnisse, in denen noch MKVs fehlen.

Beispiel:
  SRC_BASE=/media/raw SERIES_DST_BASE=/media/Serien MOVIE_DST_BASE=/media/Filme \\
  MQTT_HOST=broker MQTT_USER=user MQTT_PASSWORD=pass \\
  ./transcode/rescan.py --dry-run
"""

# Behalte Future-Annotationen, falls das Skript mal auf älteren 3.x-Umgebungen landet.
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import paho.mqtt.client as mqtt  # type: ignore

MQTT_PAYLOAD_VERSION = 3
TEMP_MKV_RE = re.compile(r"^[A-Za-z0-9]{2}_[A-Za-z][0-9]{2}\.mkv$", re.IGNORECASE)


# --------------------
# Helpers
# --------------------
def getenv(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def getenv_bool(name, default="false"):
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def is_temp_mkv(path: Path) -> bool:
    return bool(TEMP_MKV_RE.match(path.name))


def parse_source_type(value: str) -> str | None:
    if not value:
        return None
    cleaned = value.strip().lower()
    if cleaned in {"dvd", "bluray"}:
        return cleaned
    return None


def collect_source_roots(src_base: Path, default_type: str) -> List[Tuple[str, Path]]:
    roots: List[Tuple[str, Path]] = []
    for candidate in ("dvd", "bluray"):
        path = src_base / candidate
        if path.exists():
            roots.append((candidate, path))
    if roots:
        return roots
    return [(default_type, src_base)]


def find_source_type_marker(start_dir: Path, stop_dir: Path) -> str | None:
    """
    Walks up from start_dir to stop_dir (inclusive) and checks for .source_type.
    """
    current = start_dir
    stop_dir = stop_dir.resolve()
    while True:
        marker = current / ".source_type"
        if marker.is_file():
            try:
                return parse_source_type(marker.read_text().strip())
            except OSError as e:
                logging.warning("failed to read %s: %s", marker, e)
        if current == stop_dir or current.parent == current:
            break
        current = current.parent
    return None


def classify_height(height: int) -> str | None:
    if height <= 576:
        return "dvd"
    if height >= 720:
        return "bluray"
    return None


def probe_height(path: Path) -> int | None:
    try:
        out = subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=height",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        )
        height_raw = out.decode().strip()
        return int(height_raw)
    except Exception as e:
        logging.warning("ffprobe height failed for %s: %s", path, e)
        return None


def detect_source_type(
    start_dir: Path,
    stop_dir: Path,
    fallback: str,
    sample: Path,
    sample_height: int | None = None,
) -> str:
    marker = find_source_type_marker(start_dir, stop_dir)
    if marker:
        return marker
    height = sample_height
    if height is None:
        height = probe_height(sample)
    if height is not None:
        probed = classify_height(height)
        if probed:
            return probed
    return fallback


def filter_ready_mkvs(
    mkvs: List[Path],
    allow_failures: bool,
) -> Tuple[List[Path], List[Path], int | None]:
    ready: List[Path] = []
    dropped: List[Path] = []
    sample_height: int | None = None
    for mkv in mkvs:
        height = probe_height(mkv)
        if height is None:
            if allow_failures:
                ready.append(mkv)
            else:
                dropped.append(mkv)
            continue
        ready.append(mkv)
        if sample_height is None:
            sample_height = height
    return ready, dropped, sample_height


def chunk_list(items: List[Path], size: int) -> List[List[Path]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_mqtt_client() -> mqtt.Client:
    kwargs = {}
    callback_version = getattr(mqtt, "CallbackAPIVersion", None)
    if callback_version:
        kwargs["callback_api_version"] = callback_version.VERSION2

    client = mqtt.Client(**kwargs)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    if MQTT_SSL:
        client.tls_set()
        logging.info("MQTT TLS enabled")
    return client


def connect_mqtt(client: mqtt.Client):
    while True:
        try:
            logging.info("connecting to MQTT broker…")
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            logging.info("MQTT connected")
            return
        except OSError as e:
            logging.warning(f"MQTT connect failed: {e}, retrying in 5s")
            time.sleep(5)


def mqtt_publish(client: mqtt.Client, topic: str, payload: dict, dry_run: bool):
    msg = json.dumps(payload)
    if dry_run:
        logging.info("[dry-run] would publish to %s: %s", topic, msg)
        return
    client.publish(topic, msg, qos=1, retain=False)
    logging.info("published job to %s: %s", topic, msg)


def load_env_file(path: Path):
    """
    Lädt KEY=VALUE Paare aus einer Datei und setzt sie, falls sie noch nicht
    im Environment stehen.
    """
    if not path.exists():
        logging.info("env-file not found, skipping: %s", path)
        return

    logging.info("loading env vars from %s", path)
    with path.open() as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                logging.warning("ignore malformed line in env file: %s", line)
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            if key in os.environ:
                continue
            os.environ[key] = value.strip()


def collect_missing_series_dirs(
    src_base: Path, dst_base: Path
) -> Tuple[Dict[Path, List[Path]], List[Path]]:
    """
    Liefert {directory: [missing mkv files]} und eine Liste übersprungener Temp-MKVs.
    """
    missing: Dict[Path, List[Path]] = {}
    skipped: List[Path] = []

    if not src_base.exists():
        logging.warning("series source base does not exist: %s", src_base)
        return missing, skipped

    for mkv in src_base.rglob("*.mkv"):
        if is_temp_mkv(mkv):
            logging.info("skip temp mkv from scan: %s", mkv)
            skipped.append(mkv)
            continue
        rel = mkv.relative_to(src_base)
        dest = dst_base / rel
        if not dest.exists():
            missing.setdefault(mkv.parent, []).append(mkv)

    return missing, skipped


def collect_missing_movie_dirs(
    movie_src_base: Path, movie_dst_base: Path
) -> Tuple[Dict[Path, List[Path]], List[Path]]:
    """
    Liefert {directory: [mkv files]} für Movie-MKVs, deren Ziel fehlt,
    plus Liste übersprungener Temp-MKVs.
    """
    missing: Dict[Path, List[Path]] = {}
    skipped: List[Path] = []

    if not movie_src_base.exists():
        logging.info("movie source base does not exist, skipping: %s", movie_src_base)
        return missing, skipped

    for mkv in movie_src_base.rglob("*.mkv"):
        if is_temp_mkv(mkv):
            logging.info("skip temp mkv from scan: %s", mkv)
            skipped.append(mkv)
            continue
        rel = mkv.relative_to(movie_src_base)
        dest = movie_dst_base / rel
        if dest.exists():
            continue
        missing.setdefault(mkv.parent, []).append(mkv)

    return missing, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Finde fehlende Transcodes und publiziere MQTT-Jobs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="nur anzeigen, welche Jobs gesendet würden",
    )
    parser.add_argument(
        "--env-file",
        default="/etc/transcode-mqtt.env",
        help="Pfad zu einer KEY=VALUE Env-Datei (überschreibt fehlende Variablen)",
    )
    parser.add_argument(
        "--allow-ffprobe-failures",
        action="store_true",
        help="FFprobe-Fehler ignorieren und Dateien trotzdem publishen",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Config (ENV) – zuerst optionale Env-Datei laden
    load_env_file(Path(args.env_file))

    global MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASSWORD, MQTT_SSL
    MQTT_HOST = getenv("MQTT_HOST", required=True)
    MQTT_PORT = int(getenv("MQTT_PORT", "1883"))
    MQTT_USER = getenv("MQTT_USER", required=True)
    MQTT_PASSWORD = getenv("MQTT_PASSWORD", required=True)
    MQTT_TOPIC = getenv("MQTT_TOPIC", "media/rip/done")
    MQTT_SSL = getenv_bool("MQTT_SSL", "false")

    src_base = Path(getenv("SRC_BASE", required=True)).expanduser().resolve()
    source_type_default = getenv("SOURCE_TYPE", "dvd").strip().lower()
    if source_type_default not in {"dvd", "bluray"}:
        raise RuntimeError("SOURCE_TYPE must be 'dvd' or 'bluray'")

    series_subpath = Path(getenv("SERIES_SUBPATH", "Serien"))
    if series_subpath.is_absolute():
        raise RuntimeError("SERIES_SUBPATH must be relative")
    series_dst_base = (
        Path(getenv("SERIES_DST_BASE", "/media/Serien")).expanduser().resolve()
    )

    movie_subpath = Path(getenv("MOVIE_SUBPATH", "Filme"))
    if movie_subpath.is_absolute():
        raise RuntimeError("MOVIE_SUBPATH must be relative")
    movie_dst_base = (
        Path(getenv("MOVIE_DST_BASE", "/media/Filme")).expanduser().resolve()
    )

    source_roots = collect_source_roots(src_base, source_type_default)
    roots_label = ", ".join(f"{stype}={path}" for stype, path in source_roots)
    logging.info(
        "config: MQTT_TOPIC=%s, SRC_BASE=%s (roots=%s, series subpath=%s, movie subpath=%s), "
        "SERIES_DST_BASE=%s, MOVIE_DST_BASE=%s",
        MQTT_TOPIC,
        src_base,
        roots_label,
        series_subpath,
        movie_subpath,
        series_dst_base,
        movie_dst_base,
    )

    scan_results = []
    series_skipped_all: List[Path] = []
    movie_skipped_all: List[Path] = []
    for source_type, source_root in source_roots:
        series_src_base = (source_root / series_subpath).resolve()
        movie_src_base = (source_root / movie_subpath).resolve()
        series_dirs, series_skipped = collect_missing_series_dirs(
            series_src_base, series_dst_base
        )
        movie_dirs, movie_skipped = collect_missing_movie_dirs(
            movie_src_base, movie_dst_base
        )
        scan_results.append(
            {
                "source_type": source_type,
                "source_root": source_root,
                "series_dirs": series_dirs,
                "movie_dirs": movie_dirs,
            }
        )
        series_skipped_all.extend(series_skipped)
        movie_skipped_all.extend(movie_skipped)

    series_total = sum(len(result["series_dirs"]) for result in scan_results)
    movie_total = sum(len(result["movie_dirs"]) for result in scan_results)

    if not series_total and not movie_total:
        logging.info("no missing transcodes detected")
        if series_skipped_all or movie_skipped_all:
            logging.info(
                "skipped temp files: %s",
                ", ".join(
                    str(p) for p in sorted(series_skipped_all + movie_skipped_all)
                ),
            )
        return

    logging.info(
        "found %d series dirs and %d movie dirs with missing outputs",
        series_total,
        movie_total,
    )

    if series_skipped_all or movie_skipped_all:
        logging.info(
            "skipped temp files: %s",
            ", ".join(str(p) for p in sorted(series_skipped_all + movie_skipped_all)),
        )

    client = build_mqtt_client()
    connect_mqtt(client)

    for result in scan_results:
        source_root = result["source_root"]
        source_type_default = result["source_type"]
        for src_dir, mkvs in sorted(result["series_dirs"].items()):
            ready_mkvs, dropped_mkvs, sample_height = filter_ready_mkvs(
                mkvs, args.allow_ffprobe_failures
            )
            if dropped_mkvs and not args.allow_ffprobe_failures:
                logging.info(
                    "dropping %d files from %s due to ffprobe errors: %s",
                    len(dropped_mkvs),
                    src_dir,
                    ", ".join(str(p) for p in dropped_mkvs),
                )
            if not ready_mkvs:
                logging.info(
                    "skip %s because all files failed ffprobe",
                    src_dir,
                )
                continue
            source_type = detect_source_type(
                src_dir, source_root, source_type_default, ready_mkvs[0], sample_height
            )
            for batch in chunk_list(ready_mkvs, 5):
                payload = {
                    "version": MQTT_PAYLOAD_VERSION,
                    "mode": "series",
                    "source_type": source_type,
                    "path": str(src_dir.resolve()),
                    "files": [str(p.resolve()) for p in batch],
                    "interlaced": None,
                }
                mqtt_publish(client, MQTT_TOPIC, payload, args.dry_run)

        for parent, mkvs in sorted(result["movie_dirs"].items()):
            ready_mkvs, dropped_mkvs, sample_height = filter_ready_mkvs(
                mkvs, args.allow_ffprobe_failures
            )
            if dropped_mkvs and not args.allow_ffprobe_failures:
                logging.info(
                    "dropping %d files from %s due to ffprobe errors: %s",
                    len(dropped_mkvs),
                    parent,
                    ", ".join(str(p) for p in dropped_mkvs),
                )
            if not ready_mkvs:
                logging.info(
                    "skip %s because all files failed ffprobe",
                    parent,
                )
                continue
            source_type = detect_source_type(
                parent, source_root, source_type_default, ready_mkvs[0], sample_height
            )
            for batch in chunk_list(ready_mkvs, 5):
                payload = {
                    "version": MQTT_PAYLOAD_VERSION,
                    "mode": "movie",
                    "source_type": source_type,
                    "path": str(parent.resolve()),
                    "files": [str(p.resolve()) for p in batch],
                    "interlaced": None,
                }
                mqtt_publish(client, MQTT_TOPIC, payload, args.dry_run)

    client.disconnect()
    logging.info("done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
