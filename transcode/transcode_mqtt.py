#!/usr/bin/env python3

import fcntl
import json
import logging
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from subprocess import CalledProcessError

import paho.mqtt.client as mqtt  # type: ignore


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


TEMP_MKV_RE = re.compile(r"^[A-Za-z0-9]{2}_[A-Za-z][0-9]{2}\.mkv$", re.IGNORECASE)


def is_temp_mkv(path: Path) -> bool:
    return bool(TEMP_MKV_RE.match(path.name))


def probe_duration(path: Path) -> float | None:
    """
    Returns media duration in seconds (float) via ffprobe, or None if unavailable.
    """
    try:
        out = subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        )
        return float(out.strip()) if out else None
    except (CalledProcessError, FileNotFoundError, ValueError) as e:
        logging.warning("ffprobe duration failed for %s: %s", path, e)
        return None


def vaapi_filter_for(path: Path) -> str:
    """
    Chooses VAAPI filter chain based on field_order.
    Interlaced -> deinterlace_vaapi; otherwise plain upload.
    """
    try:
        out = subprocess.check_output(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=field_order",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        field_order = out.strip().lower()
        interlaced = {"tt", "bb", "tb", "bt"}
        if field_order in interlaced:
            logging.info(
                "detected interlaced video (%s) for %s; applying deinterlace_vaapi",
                field_order,
                path,
            )
            return "format=nv12,hwupload,deinterlace_vaapi"
    except Exception as e:
        logging.warning(
            "ffprobe field_order failed for %s: %s; using default VAAPI filter", path, e
        )
    return "format=nv12,hwupload"


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
MQTT_SSL = getenv_bool("MQTT_SSL", "false")

MQTT_TOPIC_START = getenv("MQTT_TOPIC_START", "media/transcode/start")
MQTT_TOPIC_DONE = getenv("MQTT_TOPIC_DONE", "media/transcode/done")
MQTT_TOPIC_ERROR = getenv("MQTT_TOPIC_ERROR", "media/transcode/error")
MQTT_PAYLOAD_VERSION = 2
ENABLE_SW_FALLBACK = getenv_bool("ENABLE_SW_FALLBACK", "false")


# --------------------
# Paths (ENV)
# --------------------
SRC_BASE = Path(getenv("SRC_BASE", required=True)).expanduser().resolve()

SERIES_SUBPATH = Path(getenv("SERIES_SUBPATH", "Serien"))
if SERIES_SUBPATH.is_absolute():
    raise RuntimeError("SERIES_SUBPATH must be relative")
SERIES_SRC_BASE = (SRC_BASE / SERIES_SUBPATH).resolve()
SERIES_DST_BASE = (
    Path(getenv("SERIES_DST_BASE", "/media/Serien")).expanduser().resolve()
)
MOVIE_DST_BASE = Path(getenv("MOVIE_DST_BASE", "/media/Filme")).expanduser().resolve()


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


def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    if MQTT_SSL:
        client.tls_set()
        logging.info("MQTT TLS enabled")
    return client


# --------------------
# Transcode Logic
# --------------------
def transcode_dir(client, job: dict):
    job_path_raw = job.get("path")
    src_dir = Path(job_path_raw).resolve() if job_path_raw else None
    mode = job.get("mode", "series")
    movie_name = job.get("movie_name")
    raw_files = job.get("files") or []
    explicit_files = [Path(p).expanduser().resolve() for p in raw_files]

    if src_dir and not src_dir.exists():
        logging.warning(f"job path does not exist: {src_dir}")
        return

    src_root = None
    if src_dir:
        src_root = src_dir if src_dir.is_dir() else src_dir.parent
    elif explicit_files:
        try:
            src_root = Path(os.path.commonpath(explicit_files))
        except Exception:
            src_root = None

    mkv_files = []

    if explicit_files:
        for mkv in explicit_files:
            if not mkv.exists():
                logging.warning("skip missing file from job: %s", mkv)
                continue
            if is_temp_mkv(mkv):
                logging.info("skip temp mkv from job: %s", mkv)
                continue
            mkv_files.append(mkv)
    elif src_dir:
        if src_dir.is_file():
            if src_dir.suffix.lower() == ".mkv":
                mkv_files.append(src_dir)
            else:
                logging.warning("job path is a file but not an MKV: %s", src_dir)
        else:
            mkv_files = []
            for p in src_dir.rglob("*.mkv"):
                if is_temp_mkv(p):
                    logging.info("skip temp mkv from scan: %s", p)
                    continue
                mkv_files.append(p)
    else:
        logging.warning("job without path or files, skipping")
        return

    if not mkv_files:
        logging.info(f"no MKV files found in {src_dir or 'job list'}")
        return

    did_work = False
    multi_movie_titles = mode == "movie" and len(mkv_files) > 1

    for mkv in mkv_files:
        if mode == "movie":
            if movie_name and not multi_movie_titles:
                out = MOVIE_DST_BASE / f"{movie_name}{mkv.suffix}"
            else:
                rel = None
                if src_root:
                    try:
                        rel = mkv.relative_to(src_root)
                    except ValueError:
                        rel = None
                if rel is None:
                    rel = mkv.name
                out = MOVIE_DST_BASE / rel
        else:
            try:
                rel = mkv.relative_to(SERIES_SRC_BASE)
            except ValueError:
                rel = None
                if src_root:
                    try:
                        rel = mkv.relative_to(src_root)
                    except ValueError:
                        rel = None
                if rel is None:
                    logging.warning(
                        f"{mkv} not under configured series base {SERIES_SRC_BASE}"
                    )
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

        vf_filter = vaapi_filter_for(mkv)

        base_cmd = [
            "ffmpeg",
            "-vaapi_device",
            "/dev/dri/renderD128",
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-vf",
            vf_filter,
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
        ]

        retry_cmd = [
            "ffmpeg",
            "-vaapi_device",
            "/dev/dri/renderD128",
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-vf",
            vf_filter,
            "-fflags",
            "+genpts",
            "-avoid_negative_ts",
            "make_zero",
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
        ]

        try:
            hw_failed = False

            with open("/var/lock/vaapi.lock", "w") as lock:
                logging.info("waiting for GPU lock…")
                fcntl.flock(lock, fcntl.LOCK_EX)

                max_hw_retries = 5  # retries after initial attempt
                for attempt in range(0, max_hw_retries + 1):
                    cmd = base_cmd if attempt == 0 else retry_cmd
                    label = (
                        "initial"
                        if attempt == 0
                        else f"retry {attempt}/{max_hw_retries}"
                    )
                    try:
                        subprocess.run(cmd, check=True)
                        hw_failed = False
                        break
                    except CalledProcessError as e:
                        if out.exists():
                            try:
                                out.unlink()
                            except OSError as cleanup_err:
                                logging.warning(
                                    "could not remove incomplete output %s: %s",
                                    out,
                                    cleanup_err,
                                )
                        if attempt >= max_hw_retries:
                            logging.warning(
                                "ffmpeg %s failed (rc=%s) for %s -> %s; will fall back to software transcode",
                                label,
                                e.returncode,
                                mkv,
                                out,
                            )
                            hw_failed = True
                            break
                        else:
                            logging.warning(
                                "ffmpeg %s failed (rc=%s) for %s -> %s; retrying...",
                                label,
                                e.returncode,
                                mkv,
                                out,
                            )

            if hw_failed:
                if not ENABLE_SW_FALLBACK:
                    logging.error(
                        "hardware transcode failed after retries (SW fallback disabled) for %s",
                        mkv,
                    )
                    mqtt_publish(
                        client,
                        MQTT_TOPIC_ERROR,
                        {
                            "version": MQTT_PAYLOAD_VERSION,
                            "file": str(mkv),
                            "error": "hardware transcode failed (SW fallback disabled)",
                            "ts": int(time.time()),
                        },
                    )
                    continue

                if out.exists():
                    try:
                        out.unlink()
                    except OSError as cleanup_err:
                        logging.warning(
                            "could not remove failed output %s: %s",
                            out,
                            cleanup_err,
                        )

                sw_cmd = [
                    "ffmpeg",
                    "-i",
                    str(mkv),
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a",
                    "-map",
                    "0:s?",
                    "-c:v",
                    "libx265",
                    "-crf",
                    "22",
                    "-c:a",
                    "copy",
                    "-c:s",
                    "copy",
                    str(out),
                ]
                subprocess.run(sw_cmd, check=True)

            in_duration = probe_duration(mkv)
            out_duration = probe_duration(out)
            if in_duration and out_duration:
                tolerance = max(1.0, in_duration * 0.01)  # 1s or 1% of input
                if abs(in_duration - out_duration) > tolerance:
                    logging.error(
                        "duration mismatch (in=%0.2fs, out=%0.2fs, tol=%0.2fs) for %s",
                        in_duration,
                        out_duration,
                        tolerance,
                        mkv,
                    )
                    try:
                        out.unlink()
                    except OSError as cleanup_err:
                        logging.warning(
                            "could not remove mismatched output %s: %s",
                            out,
                            cleanup_err,
                        )
                    raise RuntimeError("duration mismatch after transcode")

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
                job = {"path": str(job.resolve()), "mode": "series", "movie_name": None}
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

        version = payload.get("version")
        if version != MQTT_PAYLOAD_VERSION:
            logging.warning(
                f"unsupported payload version {version}, expected {MQTT_PAYLOAD_VERSION}"
            )
            return

        path = None
        if "path" in payload:
            path = Path(payload["path"]).expanduser().resolve()
            if not path.exists():
                logging.warning(f"received path does not exist: {path}")
                path = None

        files_raw = payload.get("files")
        files = []
        if not isinstance(files_raw, list) or not files_raw:
            logging.warning("v2 payload requires non-empty 'files' list, skipping")
            return
        files = [str(Path(file_path).expanduser().resolve()) for file_path in files_raw]

        mode = payload.get("mode", "series")
        movie_name = payload.get("movie_name")

        logging.info(
            "MQTT job received (mode=%s, path=%s, files=%d)",
            mode,
            path,
            len(files),
        )
        if userdata is None:
            logging.error("no job queue configured – cannot process message")
            return

        userdata.put(
            {
                "path": str(path) if path else None,
                "mode": mode,
                "movie_name": movie_name,
                "files": files,
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

    client = build_mqtt_client()
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
