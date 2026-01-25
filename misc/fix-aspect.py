#!/usr/bin/env python3
"""
Setzt das DAR (Display Aspect Ratio) in MKV-Dateien via mkvpropedit.
Arbeitet nur auf explizit angegebenen Verzeichnissen/Dateien.

Beispiele:
  ./misc/fix-aspect.py --aspect 16:9 /media/raw/dvd/Serien/Star_Trek-Enterprise/S02/02
  ./misc/fix-aspect.py --aspect 4:3 --dry-run /path/zu/datei.mkv
"""

# Behalte Future-Annotationen, falls das Skript mal auf älteren 3.x-Umgebungen landet.
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Tuple


def parse_aspect(value: str) -> Tuple[int, int]:
    cleaned = value.strip()
    if cleaned == "4:3":
        return (4, 3)
    if cleaned == "16:9":
        return (16, 9)
    raise argparse.ArgumentTypeError("aspect must be 4:3 or 16:9")


def iter_mkvs(paths: Iterable[Path], recursive: bool) -> Iterable[Path]:
    for path in paths:
        if path.is_dir():
            if recursive:
                yield from path.rglob("*.mkv")
            else:
                yield from path.glob("*.mkv")
        elif path.is_file() and path.suffix.lower() == ".mkv":
            yield path
        else:
            logging.warning("skip non-mkv path: %s", path)


def probe_video(path: Path) -> Tuple[int, int, str, str]:
    out = subprocess.check_output(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,display_aspect_ratio,sample_aspect_ratio",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        stderr=subprocess.DEVNULL,
    ).decode()
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if len(lines) < 4:
        raise RuntimeError(f"ffprobe returned incomplete data for {path}")
    width = int(lines[0])
    height = int(lines[1])
    dar = lines[2]
    sar = lines[3]
    return width, height, dar, sar


def calc_display_width(height: int, aspect: Tuple[int, int]) -> int:
    num, den = aspect
    width = round(height * num / den)
    if width % 2 == 1:
        width += 1
    return width


def apply_aspect(path: Path, aspect: Tuple[int, int], dry_run: bool) -> bool:
    width, height, dar, sar = probe_video(path)
    desired_width = calc_display_width(height, aspect)
    desired_dar = f"{aspect[0]}:{aspect[1]}"

    if dar == desired_dar:
        logging.info("skip (already %s): %s", desired_dar, path)
        return False

    cmd = [
        "mkvpropedit",
        str(path),
        "--edit",
        "track:v1",
        "--set",
        f"display-width={desired_width}",
        "--set",
        f"display-height={height}",
    ]
    logging.info(
        "fix %s: %sx%s sar=%s dar=%s -> dar=%s (display %sx%s)",
        path,
        width,
        height,
        sar,
        dar,
        desired_dar,
        desired_width,
        height,
    )
    if dry_run:
        logging.info("[dry-run] %s", " ".join(cmd))
        return True
    subprocess.run(cmd, check=True)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Setzt das Anzeige-Seitenverhaeltnis (DAR) via mkvpropedit"
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="MKV-Dateien oder Verzeichnisse mit MKVs (nur diese werden bearbeitet)",
    )
    parser.add_argument(
        "--aspect",
        type=parse_aspect,
        required=True,
        help="Ziel-DAR (4:3 oder 16:9)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Verzeichnisse rekursiv nach MKVs durchsuchen",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="nur anzeigen, welche Aenderungen ausgefuehrt wuerden",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    paths = [Path(p).expanduser().resolve() for p in args.paths]
    mkvs = list(iter_mkvs(paths, args.recursive))
    if not mkvs:
        logging.info("keine MKVs gefunden")
        return 0

    changed = 0
    for mkv in sorted(set(mkvs)):
        try:
            if apply_aspect(mkv, args.aspect, args.dry_run):
                changed += 1
        except Exception as e:
            logging.warning("failed to process %s: %s", mkv, e)

    logging.info("done (changed=%d)", changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
