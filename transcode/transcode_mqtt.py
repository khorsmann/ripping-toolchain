#!/usr/bin/env python3

import fcntl
import json
import logging
import os
import queue
import re
import sqlite3
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
IDET_SINGLE_RE = re.compile(
    r"Single frame detection:\s*TFF:\s*(\d+)\s*BFF:\s*(\d+)\s*Progressive:\s*(\d+)\s*Undetermined:\s*(\d+)"
)
IDET_MULTI_RE = re.compile(
    r"Multi frame detection:\s*TFF:\s*(\d+)\s*BFF:\s*(\d+)\s*Progressive:\s*(\d+)\s*Undetermined:\s*(\d+)"
)


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


def parse_idet_counts(output: str) -> tuple[int, int, int, int] | None:
    multi = IDET_MULTI_RE.search(output)
    if multi:
        return tuple(int(val) for val in multi.groups())
    single = IDET_SINGLE_RE.search(output)
    if single:
        return tuple(int(val) for val in single.groups())
    return None


def run_idet(path: Path, frames: int) -> tuple[int, int, int, int] | None:
    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-v",
        "info",
        "-i",
        str(path),
        "-an",
        "-vf",
        "idet",
        "-frames:v",
        str(frames),
        "-f",
        "null",
        "-",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        logging.warning("ffmpeg idet failed for %s: %s", path, proc.stderr.strip())
        return None
    return parse_idet_counts(proc.stderr)


def decide_idet(counts: tuple[int, int, int, int]) -> bool | None:
    tff, bff, progressive, _undetermined = counts
    interlaced = tff + bff
    if interlaced == 0 and progressive == 0:
        return None
    if interlaced > progressive:
        return True
    if progressive > interlaced:
        return False
    return None


def format_idet_counts(counts: tuple[int, int, int, int]) -> str:
    tff, bff, progressive, undetermined = counts
    return f"TFF={tff} BFF={bff} Progressive={progressive} Undetermined={undetermined}"


def detect_interlaced(path: Path) -> bool | None:
    """
    Returns True if metadata or idet indicates interlaced, False if progressive,
    None when detection fails.
    """
    field_order = ""
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
    except Exception as e:
        logging.warning("ffprobe field_order failed for %s: %s", path, e)

    interlaced_meta = {"tt", "bb", "tb", "bt"}
    if field_order in interlaced_meta:
        logging.info(
            "interlace decision: interlaced (reason=meta-data field_order=%s) for %s",
            field_order,
            path,
        )
        return True

    frames = max(50, int(getenv("IDET_FRAMES", "500")))
    idet_counts = run_idet(path, frames)
    idet_decision = decide_idet(idet_counts) if idet_counts else None

    if idet_decision is True:
        logging.info(
            "interlace decision: interlaced (reason=idet %s) for %s",
            format_idet_counts(idet_counts),
            path,
        )
        return True
    if idet_decision is False:
        logging.info(
            "interlace decision: progressive (reason=idet %s) for %s",
            format_idet_counts(idet_counts),
            path,
        )
        return False

    if field_order == "progressive":
        logging.info(
            "interlace decision: progressive (reason=meta-data field_order=progressive) for %s",
            path,
        )
        return False
    if field_order in {"unknown", ""}:
        logging.info(
            "interlace decision: interlaced (reason=meta-data field_order=%s) for %s",
            field_order or "unknown",
            path,
        )
        return True
    if field_order:
        logging.info(
            "interlace decision: progressive (reason=meta-data field_order=%s) for %s",
            field_order,
            path,
        )
        return False
    return None


def probe_audio_streams(path: Path) -> list[dict]:
    """
    Returns audio streams with index, channels, and language tags (if present).
    """
    try:
        out = subprocess.check_output(
            [
                FFPROBE_BIN,
                "-v",
                "error",
                "-select_streams",
                "a",
                "-show_entries",
                "stream=index,channels:stream_tags=language",
                "-of",
                "json",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        data = json.loads(out)
        streams = []
        for stream in data.get("streams", []):
            streams.append(
                {
                    "index": stream.get("index"),
                    "channels": stream.get("channels"),
                    "language": (stream.get("tags", {}) or {}).get("language"),
                }
            )
        return streams
    except Exception as e:
        logging.warning("ffprobe audio streams failed for %s: %s", path, e)
        return []


def probe_subtitle_streams(path: Path) -> list[dict]:
    """
    Returns subtitle streams with index and language tags (if present).
    """
    try:
        out = subprocess.check_output(
            [
                FFPROBE_BIN,
                "-v",
                "error",
                "-select_streams",
                "s",
                "-show_entries",
                "stream=index:stream_tags=language",
                "-of",
                "json",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        data = json.loads(out)
        streams = []
        for stream in data.get("streams", []):
            streams.append(
                {
                    "index": stream.get("index"),
                    "language": (stream.get("tags", {}) or {}).get("language"),
                }
            )
        return streams
    except Exception as e:
        logging.warning("ffprobe subtitle streams failed for %s: %s", path, e)
        return []


def probe_video_codec(path: Path) -> str | None:
    """
    Returns the codec name of the first video stream, if available.
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
                "stream=codec_name",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            stderr=subprocess.DEVNULL,
        ).decode()
        codec = out.strip().lower()
        return codec or None
    except Exception as e:
        logging.warning("ffprobe video codec failed for %s: %s", path, e)
        return None


def build_audio_args(
    output_index: int, channels: int | None, source_type: str
) -> list[str]:
    if channels is None:
        bitrate = "640k"
        logging.info("audio channels unknown, using %s for EAC3", bitrate)
    elif channels <= 2:
        bitrate = "256k"
    else:
        bitrate = "768k" if source_type == "bluray" else "640k"

    return [
        f"-c:a:{output_index}",
        "eac3",
        f"-b:a:{output_index}",
        bitrate,
    ]


def build_downmix_args(output_index: int) -> list[str]:
    return [
        f"-c:a:{output_index}",
        "aac",
        f"-b:a:{output_index}",
        "192k",
        f"-ac:a:{output_index}",
        "2",
    ]


def parse_langs(raw: str | None, default: str) -> set[str]:
    value = raw if raw and raw.strip() else default
    parts = [part.strip().lower() for part in value.split(",")]
    return {part for part in parts if part}


def filter_streams_by_language(streams: list[dict], allowed: set[str]) -> list[dict]:
    if not allowed:
        return streams
    filtered = []
    for stream in streams:
        lang = (stream.get("language") or "").lower()
        if lang in allowed:
            filtered.append(stream)
    return filtered


BWDIF_FILTER = "bwdif=mode=send_frame:parity=auto:deint=all"


def build_video_filter(interlaced: bool | None, hwupload: bool) -> str | None:
    filters = []
    if interlaced is True:
        filters.append(BWDIF_FILTER)
    if hwupload:
        filters.append("format=p010le")
        filters.append("hwupload=extra_hw_frames=64")
    if not filters:
        return None
    return ",".join(filters)


def build_sw_filter(interlaced: bool | None) -> str | None:
    if interlaced is True:
        return BWDIF_FILTER
    return None


def build_qsv_filter(interlaced: bool | None, qsv_direct: bool) -> str | None:
    deint = "1" if interlaced is True else "0"
    if qsv_direct:
        return f"vpp_qsv=deinterlace={deint}"
    return f"format=nv12,hwupload=extra_hw_frames=64,vpp_qsv=deinterlace={deint}"


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
AUDIO_MODE = getenv("AUDIO_MODE", "auto").strip().lower()
QSV_DIRECT = getenv_bool("QSV_DIRECT", "false")
if AUDIO_MODE not in {"auto", "encode", "copy"}:
    raise RuntimeError("AUDIO_MODE must be 'auto', 'encode' or 'copy'")
AUDIO_LANGS = parse_langs(getenv("AUDIO_LANGS"), "eng,ger,deu")
SUB_LANGS = parse_langs(getenv("SUB_LANGS"), "eng,ger,deu")
JOB_QUEUE_BACKEND = getenv("JOB_QUEUE_BACKEND", "memory").strip().lower()
JOB_QUEUE_SQLITE_PATH = Path(
    getenv("JOB_QUEUE_SQLITE_PATH", "/tmp/transcode-mqtt-jobs.sqlite3")
).expanduser()
JOB_QUEUE_POLL_INTERVAL = max(0.1, float(getenv("JOB_QUEUE_POLL_INTERVAL", "1.0")))
JOB_QUEUE_CLAIM_TTL = max(5, int(getenv("JOB_QUEUE_CLAIM_TTL", "300")))
if JOB_QUEUE_BACKEND not in {"memory", "sqlite"}:
    raise RuntimeError("JOB_QUEUE_BACKEND must be 'memory' or 'sqlite'")


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
MOVIE_SUBPATH = Path(getenv("MOVIE_SUBPATH", "Filme"))
if MOVIE_SUBPATH.is_absolute():
    raise RuntimeError("MOVIE_SUBPATH must be relative")
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


def infer_mode_from_path(path: Path) -> str | None:
    parts = {part.lower() for part in path.parts}
    if MOVIE_SUBPATH.name.lower() in parts:
        return "movie"
    if SERIES_SUBPATH.name.lower() in parts:
        return "series"
    return None


def infer_source_type_from_path(path: Path) -> str | None:
    parts = {part.lower() for part in path.parts}
    if "bluray" in parts:
        return "bluray"
    if "dvd" in parts:
        return "dvd"
    return None


class SQLiteJobQueue:
    def __init__(
        self, db_path: Path, poll_interval: float = 1.0, claim_ttl_seconds: int = 300
    ):
        self.db_path = db_path.resolve()
        self.poll_interval = poll_interval
        self.claim_ttl_seconds = claim_ttl_seconds
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL,
                created_ts INTEGER NOT NULL,
                claimed_ts INTEGER
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_claimed_id ON jobs (claimed_ts, id)"
        )
        self.conn.commit()
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)

    def put(self, job: dict):
        payload = json.dumps(job, separators=(",", ":"))
        now = int(time.time())
        with self.not_empty:
            self.conn.execute(
                "INSERT INTO jobs (payload, created_ts, claimed_ts) VALUES (?, ?, NULL)",
                (payload, now),
            )
            self.conn.commit()
            self.not_empty.notify()

    def get(self):
        while True:
            with self.not_empty:
                claimed = self._claim_next_job()
                if claimed is not None:
                    return claimed
                self.not_empty.wait(timeout=self.poll_interval)

    def _claim_next_job(self):
        reclaim_before = int(time.time()) - self.claim_ttl_seconds
        row = self.conn.execute(
            """
            SELECT id, payload
            FROM jobs
            WHERE claimed_ts IS NULL OR claimed_ts < ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (reclaim_before,),
        ).fetchone()
        if row is None:
            return None

        job_id = int(row[0])
        claimed_ts = int(time.time())
        cur = self.conn.execute(
            """
            UPDATE jobs
            SET claimed_ts = ?
            WHERE id = ? AND (claimed_ts IS NULL OR claimed_ts < ?)
            """,
            (claimed_ts, job_id, reclaim_before),
        )
        if cur.rowcount != 1:
            self.conn.commit()
            return None
        self.conn.commit()

        try:
            payload = json.loads(row[1])
        except Exception:
            logging.exception("invalid queued payload in SQLite queue, dropping id=%s", job_id)
            self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            self.conn.commit()
            return None

        if isinstance(payload, dict):
            payload["_queue_id"] = job_id
            return payload
        logging.warning("queued payload is not an object, dropping id=%s", job_id)
        self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self.conn.commit()
        return None

    def task_done(self, job: dict):
        job_id = job.get("_queue_id") if isinstance(job, dict) else None
        if job_id is None:
            logging.warning("SQLite queue task_done without _queue_id")
            return
        with self.lock:
            self.conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            self.conn.commit()


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

        if interlaced is True:
            interlaced_effective = True
        elif interlaced is False:
            interlaced_effective = False
        else:
            interlaced_effective = detect_interlaced(mkv)

        video_codec = probe_video_codec(mkv)
        audio_streams = probe_audio_streams(mkv)
        subtitle_streams = probe_subtitle_streams(mkv)
        selected_audio = filter_streams_by_language(audio_streams, AUDIO_LANGS)
        selected_subs = filter_streams_by_language(subtitle_streams, SUB_LANGS)

        if audio_streams and not selected_audio:
            logging.warning(
                "no audio streams matched language filter %s, keeping all audio",
                sorted(AUDIO_LANGS),
            )
            selected_audio = audio_streams

        audio_mode_effective = AUDIO_MODE
        if audio_mode_effective == "auto":
            audio_mode_effective = "encode" if source_type == "bluray" else "copy"

        add_downmix = ENABLE_AAC_DOWNMIX and audio_mode_effective != "copy"
        if audio_mode_effective == "copy" and ENABLE_AAC_DOWNMIX:
            logging.warning("AUDIO_MODE=copy disables audio downmix")

        maps = ["-map", "0:v:0"]
        audio_args: list[str] = []
        output_audio_index = 0
        for stream in selected_audio:
            stream_index = stream.get("index")
            if stream_index is None:
                continue
            maps.extend(["-map", f"0:{stream_index}"])
            if audio_mode_effective == "copy":
                continue
            audio_args.extend(
                build_audio_args(
                    output_audio_index, stream.get("channels"), source_type
                )
            )
            output_audio_index += 1

        if audio_mode_effective == "copy":
            audio_args = ["-c:a", "copy"]
        elif not audio_args and selected_audio:
            audio_args = [
                "-c:a",
                "eac3",
            ]

        if add_downmix and selected_audio:
            first_stream = selected_audio[0].get("index")
            if first_stream is not None:
                maps.extend(["-map", f"0:{first_stream}"])
                audio_args.extend(build_downmix_args(output_audio_index))
                output_audio_index += 1

        for stream in selected_subs:
            stream_index = stream.get("index")
            if stream_index is None:
                continue
            maps.extend(["-map", f"0:{stream_index}"])

        qsv_global_quality = 21 if source_type == "bluray" else 25
        vaapi_qp = 22 if source_type == "bluray" else 26
        x265_crf = 21 if source_type == "bluray" else 25

        def build_qsv_cmd() -> list[str]:
            cmd = [
                FFMPEG_BIN,
            ]
            if QSV_DIRECT:
                cmd.extend(
                    [
                        "-hwaccel",
                        "qsv",
                        "-qsv_device",
                        "/dev/dri/renderD128",
                        "-hwaccel_output_format",
                        "qsv",
                    ]
                )
            else:
                cmd.extend(
                    [
                        "-init_hw_device",
                        "vaapi=va:/dev/dri/renderD128",
                        "-init_hw_device",
                        "qsv=qsv@va",
                        "-filter_hw_device",
                        "qsv",
                    ]
                )
            cmd.extend(
                [
                    "-i",
                    str(mkv),
                ]
            )
            vf = build_qsv_filter(interlaced_effective, QSV_DIRECT)
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
                if video_codec == "vc1":
                    logging.info("vc1 source detected, skipping qsv decode")
                    encoders = [
                        ("vaapi", build_vaapi_cmd),
                    ]
                else:
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
                        if attempt == 0:
                            mqtt_publish(
                                client,
                                MQTT_TOPIC_START,
                                {
                                    "version": MQTT_PAYLOAD_VERSION,
                                    "file": str(mkv),
                                    "output": str(out),
                                    "encoder": encoder_label,
                                    "ts": int(time.time()),
                                },
                            )
                        logging.info("running ffmpeg with encoder %s", encoder_label)
                        logging.info("ffmpeg cmd: %s", " ".join(cmd))
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

                mqtt_publish(
                    client,
                    MQTT_TOPIC_START,
                    {
                        "version": MQTT_PAYLOAD_VERSION,
                        "file": str(mkv),
                        "output": str(out),
                        "encoder": "software",
                        "ts": int(time.time()),
                    },
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
def worker_loop(client: mqtt.Client, job_queue):
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
            if isinstance(job_queue, SQLiteJobQueue):
                job_queue.task_done(job)
            else:
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
        if isinstance(files_raw, list) and files_raw:
            files = [str(Path(file_path).expanduser().resolve()) for file_path in files_raw]
        elif path is None:
            logging.warning("payload requires 'files' list or existing 'path', skipping")
            return

        mode = payload.get("mode")
        if mode not in {"movie", "series"}:
            mode = infer_mode_from_path(path) if path else None
        if mode not in {"movie", "series"}:
            logging.warning("payload has invalid or missing mode '%s', skipping", mode)
            return

        source_type = payload.get("source_type")
        if source_type not in {"dvd", "bluray"}:
            source_type = infer_source_type_from_path(path) if path else None
        if source_type not in {"dvd", "bluray"}:
            logging.warning(
                "payload has invalid or missing source_type '%s', skipping", source_type
            )
            return
        interlaced = payload.get("interlaced")
        if interlaced is not None and not isinstance(interlaced, bool):
            logging.warning(
                "payload has invalid interlaced flag '%s', skipping", interlaced
            )
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
        "config: SRC_BASE=%s (series subpath=%s, movie subpath=%s), SERIES_DST_BASE=%s, "
        "MOVIE_DST_BASE=%s, MQTT_TOPIC=%s, JOB_QUEUE_BACKEND=%s",
        SRC_BASE,
        SERIES_SUBPATH,
        MOVIE_SUBPATH,
        SERIES_DST_BASE,
        MOVIE_DST_BASE,
        MQTT_TOPIC,
        JOB_QUEUE_BACKEND,
    )

    client = build_mqtt_client()
    if JOB_QUEUE_BACKEND == "sqlite":
        job_queue = SQLiteJobQueue(
            JOB_QUEUE_SQLITE_PATH,
            poll_interval=JOB_QUEUE_POLL_INTERVAL,
            claim_ttl_seconds=JOB_QUEUE_CLAIM_TTL,
        )
        logging.info(
            "using SQLite queue at %s (poll_interval=%ss, claim_ttl=%ss)",
            JOB_QUEUE_SQLITE_PATH,
            JOB_QUEUE_POLL_INTERVAL,
            JOB_QUEUE_CLAIM_TTL,
        )
    else:
        job_queue = queue.Queue()
        logging.info("using in-memory queue backend")
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
