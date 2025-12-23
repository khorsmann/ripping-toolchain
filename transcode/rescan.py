#!/usr/bin/env python3
"""
Vergleicht die Raw-Struktur (SRC_BASE) mit dem transkodierten Ziel (SERIES_DST_BASE/MOVIE_DST_BASE)
und publiziert MQTT-Jobs für alle Quell-Verzeichnisse, in denen noch MKVs fehlen.

Beispiel:
  SRC_BASE=/media/raw/dvd SERIES_DST_BASE=/media/Serien MOVIE_DST_BASE=/media/Filme \\
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
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import paho.mqtt.client as mqtt  # type: ignore

MQTT_PAYLOAD_VERSION = 2
TEMP_MKV_RE = re.compile(r"^[A-Za-z0-9]{2}_[A-Za-z][0-9]{2}\\.mkv$")


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
            skipped.append(mkv)
            continue
        rel = mkv.relative_to(movie_src_base)
        dest = movie_dst_base / rel
        if dest.exists():
            continue
        missing.setdefault(mkv.parent, []).append(mkv)

    return missing, skipped


def build_movie_name(
    mkvs: List[Path], parent: Path, movie_src_base: Path
) -> str | None:
    """
    Versucht einen movie_name zu bestimmen, falls genau eine Datei in der
    Quell-Directory liegt und diese direkt unter movie_src_base abgelegt ist.
    """
    if len(mkvs) == 1 and parent == movie_src_base:
        return mkvs[0].stem
    return None


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

    series_subpath = Path(getenv("SERIES_SUBPATH", "Serien"))
    if series_subpath.is_absolute():
        raise RuntimeError("SERIES_SUBPATH must be relative")
    series_src_base = (src_base / series_subpath).resolve()
    series_dst_base = (
        Path(getenv("SERIES_DST_BASE", "/media/Serien")).expanduser().resolve()
    )

    movie_subpath = Path(getenv("MOVIE_SUBPATH", "Filme"))
    if movie_subpath.is_absolute():
        raise RuntimeError("MOVIE_SUBPATH must be relative")
    movie_src_base = (src_base / movie_subpath).resolve()
    movie_dst_base = (
        Path(getenv("MOVIE_DST_BASE", "/media/Filme")).expanduser().resolve()
    )

    logging.info(
        "config: MQTT_TOPIC=%s, SRC_BASE=%s (series subpath=%s, movie subpath=%s), "
        "SERIES_DST_BASE=%s, MOVIE_DST_BASE=%s",
        MQTT_TOPIC,
        src_base,
        series_subpath,
        movie_subpath,
        series_dst_base,
        movie_dst_base,
    )

    series_dirs, series_skipped = collect_missing_series_dirs(
        series_src_base, series_dst_base
    )
    movie_dirs, movie_skipped = collect_missing_movie_dirs(
        movie_src_base, movie_dst_base
    )

    if not series_dirs and not movie_dirs:
        logging.info("no missing transcodes detected")
        if series_skipped or movie_skipped:
            logging.info(
                "skipped temp files: %s",
                ", ".join(str(p) for p in sorted(series_skipped + movie_skipped)),
            )
        return

    logging.info(
        "found %d series dirs and %d movie dirs with missing outputs",
        len(series_dirs),
        len(movie_dirs),
    )

    if series_skipped or movie_skipped:
        logging.info(
            "skipped temp files: %s",
            ", ".join(str(p) for p in sorted(series_skipped + movie_skipped)),
        )

    client = build_mqtt_client()
    connect_mqtt(client)

    for src_dir, mkvs in sorted(series_dirs.items()):
        payload = {
            "version": MQTT_PAYLOAD_VERSION,
            "mode": "series",
            "files": [str(p.resolve()) for p in mkvs],
        }
        mqtt_publish(client, MQTT_TOPIC, payload, args.dry_run)

    for parent, mkvs in sorted(movie_dirs.items()):
        movie_name = build_movie_name(mkvs, parent, movie_src_base)
        payload = {
            "version": MQTT_PAYLOAD_VERSION,
            "mode": "movie",
            "files": [str(p.resolve()) for p in mkvs],
        }
        if movie_name:
            payload["movie_name"] = movie_name
        mqtt_publish(client, MQTT_TOPIC, payload, args.dry_run)

    client.disconnect()
    logging.info("done")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
