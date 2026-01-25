#!/usr/bin/env python3
"""
Zeigt Titel-Infos aus einer .info Datei (Makemkv TINFO) an.
- Nutzt die Parser-Logik aus ripper.py
- Optionales Filtern nach Mindest-/Maximaldauer (Minuten)
"""

import argparse
from pathlib import Path
from typing import List

import ripper


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Anzeige der Makemkv-Titelinfos aus einer .info Datei"
    )
    parser.add_argument("info_file", help="Pfad zur .info Datei (Makemkv output)")
    parser.add_argument(
        "--min-minutes",
        type=int,
        default=None,
        help="Nur Titel ab dieser Dauer (Minuten)",
    )
    parser.add_argument(
        "--max-minutes",
        type=int,
        default=None,
        help="Nur Titel bis zu dieser Dauer (Minuten)",
    )
    args = parser.parse_args()

    info_path = Path(args.info_file).expanduser()
    if not info_path.exists():
        parser.error(f"Info-Datei nicht gefunden: {info_path}")

    info_text = info_path.read_text(errors="ignore")
    titles = ripper.parse_titles(info_text)

    filtered: List[dict] = []
    for t in titles:
        mins = t.get("minutes")
        if mins is None:
            continue
        if args.min_minutes is not None and mins < args.min_minutes:
            continue
        if args.max_minutes is not None and mins > args.max_minutes:
            continue
        filtered.append(t)

    print(f"Found {len(filtered)} titles (of {len(titles)} total)")
    if not filtered:
        return 0

    for t in filtered:
        chapters = t.get("chapters", "")
        chapters_txt = f" chapters={chapters}" if chapters else ""
        print(
            f"id={t['title_id']:02d} duration={t['duration']:>8} minutes={t['minutes']:3d}{chapters_txt}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
