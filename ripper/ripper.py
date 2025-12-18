#!/usr/bin/env python3
"""
DVD-Ripper für MakeMKV (CLI) + MQTT-Trigger
- Liest TINFO-Zeilen robust per Regex
- Filtert "episodenartige" Titel nach Dauer
- Rippt alle passenden Titel mit MakeMKV
- Sendet am Ende eine MQTT-Nachricht
"""
from pathlib import Path
import argparse
import json
import re
import socket
import subprocess
import sys
import time

import paho.mqtt.client as mqtt

MQTT_PAYLOAD_VERSION = 1
try:  # Python 3.11+ ships tomllib, tomli is fallback for older versions
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - tomli only if tomllib missing
    import tomli as tomllib  # type: ignore


# =====================
# KONFIGURATION
# =====================


def load_config(path: Path) -> dict:
    """
    Lädt TOML-Konfiguration und stellt Typen sicher.
    """
    config_path = path.expanduser()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    required_sections = ("mqtt", "dvd", "storage", "heuristics")
    missing = [section for section in required_sections if section not in data]
    if missing:
        raise ValueError(f"Missing config sections: {', '.join(missing)}")

    data["storage"]["base_raw"] = Path(data["storage"]["base_raw"])
    movie_path = data["storage"].get("movie_path", "Movies")
    movie_subpath = Path(movie_path)
    if movie_subpath.is_absolute():
        raise ValueError("storage.movie_path must be a relative path")
    data["storage"]["movie_path"] = movie_subpath
    data["heuristics"]["min_episode_minutes"] = int(
        data["heuristics"]["min_episode_minutes"]
    )
    max_minutes = data["heuristics"].get("max_episode_minutes")
    if max_minutes is not None:
        data["heuristics"]["max_episode_minutes"] = int(max_minutes)
    data["mqtt"]["port"] = int(data["mqtt"]["port"])

    return data


# =====================
# HILFSFUNKTIONEN
# =====================


def run(cmd):
    print("▶", " ".join(cmd))
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


# TINFO-Zeilen sehen z.B. so aus:
# TINFO:0,9,0,"0:43:31"
TINFO_RE = re.compile(r'^TINFO:(?P<title>\d+),(?P<key>\d+),\d+,"(?P<value>.*)"$')


def parse_duration_to_minutes(dur: str) -> int:
    """
    akzeptiert "H:MM:SS" oder "MM:SS"
    """
    parts = dur.split(":")
    if len(parts) == 3:
        h, m, s = map(int, parts)
    elif len(parts) == 2:
        h = 0
        m, s = map(int, parts)
    else:
        return 0
    return h * 60 + m


def parse_titles(info_text: str):
    """
    Extrahiert Titel-Metadaten aus der MakeMKV-Info-Ausgabe.
    Liefert Liste von dicts:
    {
      "title_id": int,
      "duration": "0:43:31",
      "minutes": 43,
      "chapters": "1-8" (optional)
    }
    """
    titles = {}

    for line in info_text.splitlines():
        m = TINFO_RE.match(line)
        if not m:
            continue

        tid = int(m.group("title"))
        key = int(m.group("key"))
        value = m.group("value")

        t = titles.setdefault(tid, {})

        if key == 9:  # Dauer
            t["duration"] = value
            t["minutes"] = parse_duration_to_minutes(value)
        elif key == 26:  # Kapitel-Info
            t["chapters"] = value

    result = []
    for tid, meta in titles.items():
        if "minutes" not in meta:
            continue
        result.append(
            {
                "title_id": tid,
                "duration": meta["duration"],
                "minutes": meta["minutes"],
                "chapters": meta.get("chapters", ""),
            }
        )

    # nach Titel-ID sortieren (stabil, nachvollziehbar)
    return sorted(result, key=lambda x: x["title_id"])


def sanitize_movie_name(name: str) -> str:
    """
    Normalisiert einen Movie-Namen für Dateinamen (nur Buchstaben, Zahlen, ._-).
    """
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_")
    return safe or "movie"


def dvd_device_to_disc_target(device: str) -> str:
    """
    Mappt ein Linux-Gerät (/dev/sr0, /dev/sr1, …) auf das entsprechende
    MakeMKV disc:N Target. Fällt auf disc:0 zurück, falls keine Zahl gefunden wird.
    """
    device_name = Path(device).name
    match = re.search(r"(\d+)$", device_name)
    if match:
        return f"disc:{match.group(1)}"

    print(
        f"⚠ DVD-Gerät '{device}' konnte nicht automatisch auf disc:N gemappt werden – "
        "nutze disc:0"
    )
    return "disc:0"


def mqtt_test_connection(mqtt_config: dict, timeout=5) -> bool:
    """
    Testet DNS + TCP + MQTT-Handshake.
    Gibt True zurück, wenn Verbindung möglich ist.
    """
    host = mqtt_config["host"]
    port = mqtt_config["port"]
    user = mqtt_config["user"]
    password = mqtt_config["password"]

    try:
        # DNS-Test (fängt "Name or service not known" früh ab)
        socket.gethostbyname(host)

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.username_pw_set(user, password)
        client.connect(host, port, timeout)
        client.disconnect()
        return True

    except Exception as e:
        print(f"⚠ MQTT connection test failed: {e}")
        return False


def mqtt_publish(mqtt_config: dict, payload: dict):
    """
    Publish ohne harte Exception – Fehler werden geloggt, nicht geworfen.
    """
    host = mqtt_config["host"]
    port = mqtt_config["port"]
    user = mqtt_config["user"]
    password = mqtt_config["password"]
    topic = mqtt_config["topic"]

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        client.username_pw_set(user, password)

        client.connect(host, port, 10)
        client.publish(
            topic,
            json.dumps(payload),
            qos=1,
            retain=False,
        )
        client.disconnect()

        print("📡 MQTT event published")

    except Exception as e:
        print(f"⚠ MQTT publish failed (ignored): {e}")


# =====================
# HAUPTLOGIK
# =====================


def main():
    ap = argparse.ArgumentParser(description="Bulk DVD ripper (MakeMKV + MQTT)")
    default_config = Path(__file__).with_suffix(".toml")
    ap.add_argument(
        "--series",
        help="Serien-Name, z.B. Star_Trek-Deep_Space_Nine (Pflicht außer im Film-Modus)",
    )
    ap.add_argument("--season", help="Staffel, z.B. 01 (Pflicht außer im Film-Modus)")
    ap.add_argument(
        "--disc", help="Disc-Label, z.B. disc05 (Pflicht außer im Film-Modus)"
    )
    ap.add_argument(
        "--episode-start",
        type=int,
        help="Start-Episodennummer, z.B. 15 für S01E15 (Pflicht außer im Film-Modus)",
    )
    ap.add_argument(
        "--config",
        default=str(default_config),
        help=f"Pfad zur TOML-Konfiguration (Default: {default_config.name})",
    )
    ap.add_argument(
        "--movie-name",
        help="Aktiviert den Film-Modus und legt den Dateinamen fest, z.B. 2001_ODYSSEE_IM_WELTRAUM",
    )

    args = ap.parse_args()
    movie_mode = bool(args.movie_name and args.movie_name.strip())

    if not movie_mode:
        required_fields = ("series", "season", "disc", "episode_start")
        missing = [
            f"--{field.replace('_', '-')}"
            for field in required_fields
            if getattr(args, field) is None
        ]
        if missing:
            ap.error(
                f"Missing required arguments (use --movie-name für den Film-Modus): {', '.join(missing)}"
            )

    config = load_config(Path(args.config))
    mqtt_config = config["mqtt"]
    device = config["dvd"]["device"]
    disc_target = dvd_device_to_disc_target(device)
    base_raw = config["storage"]["base_raw"]
    movie_subpath = config["storage"]["movie_path"]
    min_episode_minutes = config["heuristics"]["min_episode_minutes"]
    max_episode_minutes = (
        None if movie_mode else config["heuristics"].get("max_episode_minutes")
    )

    print("🔌 Checking MQTT connectivity…")
    mqtt_ok = mqtt_test_connection(mqtt_config)

    if not mqtt_ok:
        print("⚠ MQTT not available – ripping will continue without notification")

    if movie_mode:
        movie_name_raw = args.movie_name.strip()
        movie_name = sanitize_movie_name(movie_name_raw)
        outdir = base_raw / movie_subpath
        info_file = outdir / f"{movie_name}.info"
        movie_output = outdir / f"{movie_name}.mkv"
    else:
        outdir = base_raw / args.series / f"S{args.season}" / args.disc
        info_file = outdir / f"{args.disc}.info"
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"📀 Analyzing disc via {disc_target}…")
    info_text = run(["makemkvcon", "--noscan", "-r", "info", disc_target])
    info_file.write_text(info_text)

    titles = parse_titles(info_text)

    if not titles:
        print("⚠ Keine TINFO-Titel mit Dauer gefunden.")
        return

    # Episoden-Heuristik
    usable = [
        t
        for t in titles
        if (
            t["minutes"] >= min_episode_minutes
            and (max_episode_minutes is None or t["minutes"] <= max_episode_minutes)
        )
    ]

    if not usable:
        print("⚠ No episode-sized titles found.")
        print("   Gefundene Titel-Dauern:")
        for t in titles:
            print(f"   - title {t['title_id']}: {t['minutes']} min ({t['duration']})")
        return

    print("📋 Gefundene 'episodenartige' Titel:")
    for t in usable:
        print(f"   - title {t['title_id']}: {t['minutes']} min ({t['duration']})")

    if movie_mode:
        if movie_output.exists():
            print(f"⏭ {movie_output.name} existiert bereits, überspringe Ripping")
            episodes_ripped = 0
        else:
            movie_title = max(usable, key=lambda t: (t["minutes"], -t["title_id"]))
            tid = movie_title["title_id"]

            print(f"🎬 Ripping movie title {tid} → {movie_output}")
            run(
                ["makemkvcon", "--noscan", "-r", "mkv", disc_target, str(tid), str(outdir)]
            )

            newest = max(outdir.glob("*.mkv"), key=lambda p: p.stat().st_mtime)
            if newest != movie_output:
                newest.rename(movie_output)

            episodes_ripped = 1

    else:
        episode = args.episode_start

        for t in usable:
            tid = t["title_id"]
            filename = f"{args.series}-S{args.season}E{episode:02d}.mkv"
            out_file = outdir / filename

            if out_file.exists():
                print(f"⏭ Datei existiert bereits, überspringe: {out_file}")
                episode += 1
                continue

            print(f"🎬 Ripping title {tid} → {out_file}")
            run(
                [
                    "makemkvcon",
                    "--noscan",
                    "-r",
                    "mkv",
                    disc_target,
                    str(tid),
                    str(outdir),
                ]
            )

            # MakeMKV nennt die Datei meist anders (B1_t00.mkv).
            # Wir benennen nachträglich um:
            # finde die neueste MKV in outdir und verschiebe sie auf unseren Zielnamen.
            newest = max(outdir.glob("*.mkv"), key=lambda p: p.stat().st_mtime)
            if newest != out_file:
                newest.rename(out_file)

            episode += 1

        episodes_ripped = episode - args.episode_start

    hostname = socket.gethostname().split(".")[0]

    payload = {
        "path": str(outdir),
        "episodes": episodes_ripped,
        "hostname": hostname,
        "timestamp": int(time.time()),
        "mode": "movie" if movie_mode else "series",
        "version": MQTT_PAYLOAD_VERSION,
    }

    if movie_mode:
        payload["series"] = movie_name
        payload["season"] = "00"
        payload["disc"] = movie_name
        payload["movie_name"] = movie_name
    else:
        payload["series"] = args.series
        payload["season"] = args.season
        payload["disc"] = args.disc

    print("⏏ Ejecting disc…")
    subprocess.run(["eject", device], check=False)

    print("📡 Publishing MQTT event…")
    mqtt_publish(mqtt_config, payload)

    print("✅ Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Aborted by user")
        sys.exit(130)
    except Exception as e:
        print("❌ ERROR:", e)
        sys.exit(1)
