#!/usr/bin/env python3

from pathlib import Path
import fcntl
import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time

import paho.mqtt.client as mqtt  # type: ignore


# --------------------
# Helpers
# --------------------
def getenv(name, default=None, required=False):
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


# --------------------
# Logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


# --------------------
# MQTT Config (ENV)
# --------------------
MQTT_HOST = getenv("MQTT_HOST", required=True)
MQTT_PORT = int(getenv("MQTT_PORT", "1883"))
MQTT_USER = getenv("MQTT_USER", required=True)
MQTT_PASSWORD = getenv("MQTT_PASSWORD", required=True)
MQTT_TOPIC = getenv("MQTT_TOPIC", "media/rip/done")

MQTT_TOPIC_START = getenv("MQTT_TOPIC_START", "media/transcode/start")
MQTT_TOPIC_DONE = getenv("MQTT_TOPIC_DONE", "media/transcode/done")
MQTT_TOPIC_ERROR = getenv("MQTT_TOPIC_ERROR", "media/transcode/error")
MQTT_PAYLOAD_VERSION = 1


# --------------------
# Paths (ENV)
# --------------------
SRC_BASE = Path(getenv("SRC_BASE", required=True))

SERIES_SUBPATH = Path(getenv("SERIES_SUBPATH", "Serien"))
if SERIES_SUBPATH.is_absolute():
    raise RuntimeError("SERIES_SUBPATH must be relative")
SERIES_SRC_BASE = SRC_BASE / SERIES_SUBPATH
SERIES_DST_BASE = Path(getenv("SERIES_DST_BASE", "/media/Serien"))
MOVIE_DST_BASE = Path(getenv("MOVIE_DST_BASE", "/media/Filme"))


# --------------------
# MQTT helpers
# --------------------
def mqtt_publish(client, topic, payload):
    client.publish(topic, json.dumps(payload), qos=1, retain=False)


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


# --------------------
# Transcode Logic
# --------------------
def transcode_dir(client, job: dict):
    src_dir = Path(job["path"])
    mode = job.get("mode", "series")
    movie_name = job.get("movie_name")

    if not src_dir.exists():
        logging.warning(f"job path does not exist: {src_dir}")
        return

    mkv_files = list(src_dir.rglob("*.mkv"))
    if not mkv_files:
        logging.info(f"no MKV files found in {src_dir}")
        return

    did_work = False
    multi_movie_titles = mode == "movie" and len(mkv_files) > 1

    for mkv in mkv_files:
        if mode == "movie":
            if movie_name and not multi_movie_titles:
                out = MOVIE_DST_BASE / f"{movie_name}{mkv.suffix}"
            else:
                rel = mkv.relative_to(src_dir)
                out = MOVIE_DST_BASE / rel
        else:
            try:
                rel = mkv.relative_to(SERIES_SRC_BASE)
            except ValueError:
                logging.warning(f"{mkv} not under configured series base {SERIES_SRC_BASE}")
                continue
            out = SERIES_DST_BASE / rel

        out.parent.mkdir(parents=True, exist_ok=True)

        if out.exists():
            logging.info(f"skip existing file: {out}")
            continue

        did_work = True
        logging.info(f"transcoding {mkv} → {out}")

        mqtt_publish(
            client,
            MQTT_TOPIC_START,
            {
                "version": MQTT_PAYLOAD_VERSION,
                "file": str(mkv),
                "output": str(out),
                "ts": int(time.time()),
            },
        )

        try:
            with open("/var/lock/vaapi.lock", "w") as lock:
                logging.info("waiting for GPU lock…")
                fcntl.flock(lock, fcntl.LOCK_EX)

                subprocess.run(
                    [
                        "ffmpeg",
                        "-vaapi_device",
                        "/dev/dri/renderD128",
                        "-hwaccel",
                        "vaapi",
                        "-hwaccel_output_format",
                        "vaapi",
                        "-i",
                        str(mkv),
                        "-map",
                        "0:v:0",
                        "-map",
                        "0:a",
                        "-map",
                        "0:s?",
                        "-c:v",
                        "hevc_vaapi",
                        "-qp",
                        "22",
                        "-c:a",
                        "copy",
                        "-c:s",
                        "copy",
                        str(out),
                    ],
                    check=True,
                )

            mqtt_publish(
                client,
                MQTT_TOPIC_DONE,
                {
                    "version": MQTT_PAYLOAD_VERSION,
                    "file": str(out),
                    "ts": int(time.time()),
                },
            )

        except Exception as e:
            mqtt_publish(
                client,
                MQTT_TOPIC_ERROR,
                {
                    "version": MQTT_PAYLOAD_VERSION,
                    "file": str(mkv),
                    "error": str(e),
                    "ts": int(time.time()),
                },
            )
            raise

    if not did_work:
        logging.info("no transcoding needed – all files already exist")


# --------------------
# Worker Thread
# --------------------
def worker_loop(client: mqtt.Client, job_queue: queue.Queue):
    while True:
        job = job_queue.get()
        try:
            if isinstance(job, Path):
                job = {"path": str(job), "mode": "series", "movie_name": None}
            logging.info(f"processing queued job for {job.get('path')}")
            transcode_dir(client, job)
        except Exception:
            logging.exception(f"transcode error while handling {job.get('path')}")
        finally:
            job_queue.task_done()


# --------------------
# MQTT callback
# --------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        if "path" not in payload:
            logging.warning(f"ignoring MQTT message without path: {payload}")
            return

        version = payload.get("version")
        if version != MQTT_PAYLOAD_VERSION:
            logging.warning(
                f"unsupported payload version {version}, expected {MQTT_PAYLOAD_VERSION}"
            )
            return

        path = Path(payload["path"])

        if not path.exists():
            logging.warning(f"received path does not exist: {path}")
            return

        mode = payload.get("mode", "series")
        movie_name = payload.get("movie_name")

        logging.info(f"MQTT job received for {path} (mode={mode})")
        if userdata is None:
            logging.error("no job queue configured – cannot process message")
            return

        userdata.put(
            {
                "path": str(path),
                "mode": mode,
                "movie_name": movie_name,
            }
        )

    except json.JSONDecodeError:
        logging.warning("invalid JSON payload received")

    except Exception:
        logging.exception("failed to enqueue MQTT job")


# --------------------
# Main
# --------------------
def main():
    logging.info("transcode-mqtt starting up")
    logging.info(
        "config: SRC_BASE=%s (series subpath=%s), SERIES_DST_BASE=%s, MOVIE_DST_BASE=%s, "
        "MQTT_TOPIC=%s",
        SRC_BASE,
        SERIES_SUBPATH,
        SERIES_DST_BASE,
        MOVIE_DST_BASE,
        MQTT_TOPIC,
    )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    job_queue = queue.Queue()
    client.user_data_set(job_queue)
    client.on_message = on_message

    worker = threading.Thread(target=worker_loop, args=(client, job_queue), daemon=True)
    worker.start()

    connect_mqtt(client)

    client.subscribe(MQTT_TOPIC, qos=1)
    logging.info("waiting for rip events…")

    client.loop_forever()


if __name__ == "__main__":
    main()
