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
                FFPROBE_BIN,
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


def detect_interlaced(path: Path) -> bool | None:
    """
    Returns True if ffprobe reports an interlaced field_order, False if not,
    None when detection fails.
    """
    try:
        out = subprocess.check_output(
            [
                FFPROBE_BIN,
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
            logging.info("detected interlaced video (%s) for %s", field_order, path)
            return True
        if field_order:
            return False
    except Exception as e:
        logging.warning("ffprobe field_order failed for %s: %s", path, e)
    return None


def probe_audio_channels(path: Path) -> int | None:
    """
    Returns channel count of the first audio stream, or None if unavailable.
    """
    try:
        out = subprocess.check_output(
            [
                FFPROBE_BIN,
                "-v",
                "error",
                "-select_streams",
                "a:0",
                "-show_entries",
                "stream=channels",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        channels_raw = out.strip()
        if not channels_raw:
            return None
        return int(channels_raw)
    except Exception as e:
        logging.warning("ffprobe audio channels failed for %s: %s", path, e)
        return None


def build_audio_args(channels: int | None, source_type: str) -> list[str]:
    if channels is None:
        bitrate = "640k" if source_type == "bluray" else "640k"
        logging.info("audio channels unknown, using %s for EAC3", bitrate)
    elif channels <= 2:
        bitrate = "256k"
    else:
        bitrate = "768k" if source_type == "bluray" else "640k"

    return [
        "-c:a:0",
        "eac3",
        "-b:a:0",
        bitrate,
    ]


def build_downmix_args() -> list[str]:
    return [
        "-c:a:1",
        "aac",
        "-b:a:1",
        "192k",
        "-ac:a:1",
        "2",
    ]


def build_audio_maps(mode: str, add_downmix: bool) -> tuple[list[str], bool]:
    if mode == "copy":
        if add_downmix:
            logging.warning("AUDIO_MODE=copy disables audio downmix")
        return ["-map", "0:a?"], False
    return ["-map", "0:a:0?"], add_downmix


def build_video_filter(interlaced: bool | None, hwupload: bool) -> str | None:
    filters = []
    if interlaced is True:
        filters.append("bwdif")
    if hwupload:
        filters.append("format=p010le")
        filters.append("hwupload=extra_hw_frames=64")
    if not filters:
        return None
    return ",".join(filters)


def build_sw_filter(interlaced: bool | None) -> str | None:
    if interlaced is True:
        return "bwdif"
    return None


def build_qsv_filter(interlaced: bool | None) -> str | None:
    if interlaced is True:
        return "vpp_qsv=deinterlace=1"
    return "vpp_qsv=deinterlace=0"


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
MQTT_PAYLOAD_VERSION = 3
ENABLE_SW_FALLBACK = getenv_bool("ENABLE_SW_FALLBACK", "true")
MAX_HW_RETRIES = max(0, int(getenv("MAX_HW_RETRIES", "2")))
ENABLE_AAC_DOWNMIX = getenv_bool("ENABLE_AAC_DOWNMIX", "false")
AUDIO_MODE = getenv("AUDIO_MODE", "copy").strip().lower()
if AUDIO_MODE not in {"encode", "copy"}:
    raise RuntimeError("AUDIO_MODE must be 'encode' or 'copy'")


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


def resolve_ffmpeg_bin() -> str:
    explicit = os.getenv("FFMPEG_BIN")
    if explicit:
        return explicit
    jellyfin = Path("/usr/lib/jellyfin-ffmpeg/ffmpeg")
    if jellyfin.exists():
        return str(jellyfin)
    return "ffmpeg"


def resolve_ffprobe_bin() -> str:
    explicit = os.getenv("FFPROBE_BIN")
    if explicit:
        return explicit
    jellyfin = Path("/usr/lib/jellyfin-ffmpeg/ffprobe")
    if jellyfin.exists():
        return str(jellyfin)
    return "ffprobe"


FFMPEG_BIN = resolve_ffmpeg_bin()
FFPROBE_BIN = resolve_ffprobe_bin()


def series_src_base_for_source(source_type: str) -> Path:
    cleaned = (source_type or "").strip().lower()
    if cleaned in {"dvd", "bluray"}:
        candidate_root = SRC_BASE / cleaned
        if candidate_root.exists():
            return (candidate_root / SERIES_SUBPATH).resolve()
    return SERIES_SRC_BASE


# --------------------
# Transcode Logic
# --------------------
def transcode_dir(client, job: dict):
    job_path_raw = job.get("path")
    src_dir = Path(job_path_raw).resolve() if job_path_raw else None
    mode = job.get("mode", "series")
    interlaced = job.get("interlaced")
    source_type = job.get("source_type", "dvd")
    raw_files = job.get("files") or []
    explicit_files = [Path(p).expanduser().resolve() for p in raw_files]
    series_src_base = series_src_base_for_source(source_type)

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
    for mkv in mkv_files:
        if mode == "movie":
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
                rel = mkv.relative_to(series_src_base)
            except ValueError:
                rel = None
                if src_root:
                    try:
                        rel = mkv.relative_to(src_root)
                    except ValueError:
                        rel = None
                if rel is None:
                    logging.warning(
                        f"{mkv} not under configured series base {series_src_base}"
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

        if interlaced is True:
            interlaced_effective = True
        elif interlaced is False:
            interlaced_effective = False
        else:
            interlaced_effective = detect_interlaced(mkv)

        audio_channels = probe_audio_channels(mkv)
        add_downmix = ENABLE_AAC_DOWNMIX
        audio_maps, add_downmix = build_audio_maps(AUDIO_MODE, add_downmix)
        if AUDIO_MODE == "copy":
            audio_args = ["-c:a", "copy"]
        else:
            audio_args = build_audio_args(audio_channels, source_type)

        maps = ["-map", "0:v:0", *audio_maps, "-map", "0:s?"]
        if add_downmix:
            maps.extend(["-map", "0:a:0?"])

        qsv_global_quality = 21 if source_type == "bluray" else 25
        vaapi_qp = 22 if source_type == "bluray" else 26
        x265_crf = 21 if source_type == "bluray" else 25

        def build_qsv_cmd() -> list[str]:
            cmd = [
                FFMPEG_BIN,
                "-hwaccel",
                "qsv",
                "-qsv_device",
                "/dev/dri/renderD128",
                "-hwaccel_output_format",
                "qsv",
                "-i",
                str(mkv),
            ]
            vf = build_qsv_filter(interlaced_effective)
            if vf:
                cmd.extend(["-vf", vf])
            cmd.extend(maps)
            cmd.extend(
                [
                    "-c:v",
                    "hevc_qsv",
                    "-profile:v",
                    "main",
                    "-global_quality",
                    str(qsv_global_quality),
                    "-pix_fmt",
                    "nv12",
                ]
            )
            cmd.extend(audio_args)
            if add_downmix:
                cmd.extend(build_downmix_args())
            cmd.extend(["-c:s", "copy", str(out)])
            return cmd

        def build_vaapi_cmd() -> list[str]:
            cmd = [
                FFMPEG_BIN,
                "-init_hw_device",
                "vaapi=va:/dev/dri/renderD128",
                "-filter_hw_device",
                "va",
                "-i",
                str(mkv),
            ]
            vf = build_video_filter(interlaced_effective, hwupload=True)
            if vf:
                cmd.extend(["-vf", vf])
            cmd.extend(maps)
            cmd.extend(
                [
                    "-c:v",
                    "hevc_vaapi",
                    "-profile:v",
                    "main10",
                    "-qp",
                    str(vaapi_qp),
                ]
            )
            cmd.extend(audio_args)
            if add_downmix:
                cmd.extend(build_downmix_args())
            cmd.extend(["-c:s", "copy", str(out)])
            return cmd

        def build_sw_cmd() -> list[str]:
            cmd = [
                FFMPEG_BIN,
                "-i",
                str(mkv),
            ]
            vf = build_sw_filter(interlaced_effective)
            if vf:
                cmd.extend(["-vf", vf])
            cmd.extend(maps)
            cmd.extend(
                [
                    "-c:v",
                    "libx265",
                    "-preset",
                    "slow",
                    "-crf",
                    str(x265_crf),
                    "-pix_fmt",
                    "yuv420p10le",
                ]
            )
            cmd.extend(audio_args)
            if add_downmix:
                cmd.extend(build_downmix_args())
            cmd.extend(["-c:s", "copy", str(out)])
            return cmd

        try:
            hw_failed = False

            with open("/var/lock/vaapi.lock", "w") as lock:
                logging.info("waiting for GPU lock…")
                fcntl.flock(lock, fcntl.LOCK_EX)

                max_hw_retries = MAX_HW_RETRIES
                encoders = [
                    ("qsv", build_qsv_cmd),
                    ("vaapi", build_vaapi_cmd),
                ]

                for encoder_label, cmd_builder in encoders:
                    for attempt in range(0, max_hw_retries + 1):
                        cmd = cmd_builder()
                        label = (
                            f"{encoder_label} initial"
                            if attempt == 0
                            else f"{encoder_label} retry {attempt}/{max_hw_retries}"
                        )
                        logging.info("running ffmpeg with encoder %s", encoder_label)
                        try:
                            subprocess.run(cmd, check=True)
                            hw_failed = False
                            break
                        except (CalledProcessError, FileNotFoundError) as e:
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
                                    "ffmpeg %s failed (%s) for %s -> %s; trying next encoder",
                                    label,
                                    getattr(e, "returncode", "error"),
                                    mkv,
                                    out,
                                )
                            else:
                                logging.warning(
                                    "ffmpeg %s failed (%s) for %s -> %s; retrying...",
                                    label,
                                    getattr(e, "returncode", "error"),
                                    mkv,
                                    out,
                                )
                                continue
                    if out.exists():
                        hw_failed = False
                        break
                    hw_failed = True

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

                sw_cmd = build_sw_cmd()
                subprocess.run(sw_cmd, check=True)

            in_duration = probe_duration(mkv)
            out_duration = probe_duration(out)
            if in_duration and out_duration:
                tolerance = max(1.0, in_duration * 0.01)  # 1s or 1% of input
                if abs(in_duration - out_duration) > tolerance:
                    logging.warning(
                        "duration mismatch (in=%0.2fs, out=%0.2fs, tol=%0.2fs) for %s (keeping output)",
                        in_duration,
                        out_duration,
                        tolerance,
                        mkv,
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
                job = {"path": str(job.resolve()), "mode": "series"}
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
        if not isinstance(version, int):
            logging.warning(
                "payload has invalid version '%s', expected %s",
                version,
                MQTT_PAYLOAD_VERSION,
            )
            return
        if version < MQTT_PAYLOAD_VERSION:
            logging.warning(
                "unsupported payload version %s, expected %s",
                version,
                MQTT_PAYLOAD_VERSION,
            )
            return
        if version != MQTT_PAYLOAD_VERSION:
            logging.warning(
                "unsupported payload version %s, expected %s",
                version,
                MQTT_PAYLOAD_VERSION,
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
            logging.warning("payload requires non-empty 'files' list, skipping")
            return
        files = [str(Path(file_path).expanduser().resolve()) for file_path in files_raw]

        mode = payload.get("mode", "series")
        if mode not in {"movie", "series"}:
            logging.warning("payload has invalid mode '%s', skipping", mode)
            return
        source_type = payload.get("source_type")
        if source_type not in {"dvd", "bluray"}:
            logging.warning("payload has invalid source_type '%s', skipping", source_type)
            return
        interlaced = payload.get("interlaced")
        if interlaced is not None and not isinstance(interlaced, bool):
            logging.warning("payload has invalid interlaced flag '%s', skipping", interlaced)
            return

        logging.info(
            "MQTT job received (mode=%s, source_type=%s, path=%s, files=%d)",
            mode,
            source_type,
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
                "source_type": source_type,
                "files": files,
                "interlaced": interlaced,
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
