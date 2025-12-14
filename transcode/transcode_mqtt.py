#!/usr/bin/env python3

from pathlib import Path
import fcntl
import json
import logging
import os
import subprocess
import sys
import time

import paho.mqtt.client as mqtt # type: ignore


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


# --------------------
# Paths (ENV)
# --------------------
SRC_BASE = Path(getenv("SRC_BASE", required=True))
DST_BASE = Path(getenv("DST_BASE", required=True))


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
def transcode_dir(client, src_dir: Path):
    did_work = False

    for mkv in src_dir.rglob("*.mkv"):
        rel = mkv.relative_to(SRC_BASE)
        out = DST_BASE / rel
        out.parent.mkdir(parents=True, exist_ok=True)

        if out.exists():
            logging.info(f"skip existing file: {out}")
            continue

        did_work = True
        logging.info(f"transcoding {mkv} → {out}")

        mqtt_publish(
            client,
            MQTT_TOPIC_START,
            {"file": str(mkv), "output": str(out), "ts": int(time.time())},
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
                {"file": str(out), "ts": int(time.time())},
            )

        except Exception as e:
            mqtt_publish(
                client,
                MQTT_TOPIC_ERROR,
                {"file": str(mkv), "error": str(e), "ts": int(time.time())},
            )
            raise

    if not did_work:
        logging.info("no transcoding needed – all files already exist")


# --------------------
# MQTT callback
# --------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())

        if "path" not in payload:
            logging.warning(f"ignoring MQTT message without path: {payload}")
            return

        path = Path(payload["path"])

        if not path.exists():
            logging.warning(f"received path does not exist: {path}")
            return

        logging.info(f"MQTT job received for {path}")
        transcode_dir(client, path)

    except json.JSONDecodeError:
        logging.warning("invalid JSON payload received")

    except Exception:
        logging.exception("transcode error")


# --------------------
# Main
# --------------------
def main():
    logging.info("transcode-mqtt starting up")
    logging.info(
        f"config: SRC_BASE={SRC_BASE}, DST_BASE={DST_BASE}, MQTT_TOPIC={MQTT_TOPIC}"
    )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_message = on_message

    connect_mqtt(client)

    client.subscribe(MQTT_TOPIC, qos=1)
    logging.info("waiting for rip events…")

    client.loop_forever()


if __name__ == "__main__":
    main()
