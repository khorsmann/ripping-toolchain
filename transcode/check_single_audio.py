#!/usr/bin/env python3
import argparse
import json
import subprocess
from pathlib import Path


def run_ffprobe(ffprobe: str, path: Path) -> list[dict]:
    cmd = [
        ffprobe,
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index,codec_name:stream_tags=language,title",
        "-of",
        "json",
        str(path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "ffprobe failed")
    data = json.loads(proc.stdout or "{}")
    return data.get("streams", [])


def format_stream(stream: dict) -> str:
    index = stream.get("index", "")
    codec = stream.get("codec_name", "")
    tags = stream.get("tags", {}) or {}
    lang = tags.get("language", "und")
    title = tags.get("title", "")
    parts = [str(index), str(codec), str(lang)]
    if title:
        parts.append(str(title))
    return ",".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="List MKV files that have only one audio stream, including audio tag info."
    )
    ap.add_argument("root", type=Path, help="Root folder to scan recursively")
    ap.add_argument("--ffprobe", default="ffprobe", help="Path to ffprobe")
    ap.add_argument(
        "--ext", default=".mkv", help="File extension filter (default .mkv)"
    )
    args = ap.parse_args()

    root = args.root
    if not root.exists():
        print(f"Root does not exist: {root}")
        return 2

    ext = args.ext.lower()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() != ext:
            continue
        try:
            streams = run_ffprobe(args.ffprobe, path)
        except Exception as exc:
            print(f"{path} | ERROR: {exc}")
            continue
        if len(streams) == 1:
            print(f"{path} | {format_stream(streams[0])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
