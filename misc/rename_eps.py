#!/usr/bin/env python3
import re
import os
from pathlib import Path
import argparse

pattern = re.compile(r"(S\d+E)(\d{2})(\.mkv)$", re.IGNORECASE)


def collect_renames(base: Path, offset: int):
    files = list(base.rglob("*.mkv"))
    renames = []

    for f in files:
        m = pattern.search(f.name)
        if not m:
            continue

        ep = int(m.group(2))
        new_ep = ep + offset

        if new_ep < 0:
            print(f"Übersprungen (negative Episode): {f.name}")
            continue

        new_name = f.name.replace(f"E{ep:02d}", f"E{new_ep:02d}")

        renames.append((ep, f, f.with_name(new_name)))

    # Sort in direction that keeps targets free to avoid overwrites
    renames.sort(reverse=offset >= 0, key=lambda x: x[0])
    return renames


def build_argparser():
    examples = r"""
Beispiele:

  Dry-Run (nur anzeigen, nichts ändern)
    rename_eps.py /pfad/zur/serie

  Um +1 hochzählen und wirklich umbenennen
    rename_eps.py /pfad/zur/serie --apply

  Um 1 herunterzählen
    rename_eps.py /pfad/zur/serie --down --apply

  Beliebiges Offset, z.B. -3
    rename_eps.py /pfad/zur/serie --offset -3 --apply
"""

    ap = argparse.ArgumentParser(
        prog="rename_eps.py",
        description=(
            "Passt Episodennummern in Dateinamen (SxxExx) per Offset an.\n"
            "Unterstützt Hoch- und Runterzählen."
        ),
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    ap.add_argument(
        "path",
        type=Path,
        help="Basisordner, in dem rekursiv nach Dateien gesucht wird",
    )

    grp = ap.add_mutually_exclusive_group()
    grp.add_argument(
        "--offset",
        type=int,
        default=1,
        help="Offset (positiv = hoch, negativ = runter; Standard: +1)",
    )
    grp.add_argument(
        "--down",
        action="store_true",
        help="Episoden um 1 herunterzählen (entspricht --offset -1)",
    )

    ap.add_argument(
        "--apply",
        action="store_true",
        help="Änderungen wirklich durchführen (ohne = Dry-Run)",
    )

    return ap


def main():
    ap = build_argparser()
    args = ap.parse_args()

    offset = -1 if args.down else args.offset

    base = args.path.resolve()
    if not base.exists():
        print(f"Pfad existiert nicht: {base}")
        return

    renames = collect_renames(base, offset)

    if not renames:
        print("Keine passenden Dateien gefunden.")
        return

    print(f"\nGeplante Umbenennungen (Offset {offset:+}):\n")

    for ep, src, dst in renames:
        print(f"{src}  ->  {dst}")
        if args.apply:
            os.rename(src, dst)

    if not args.apply:
        print(
            "\nDry-Run — nichts wurde umbenannt.\n"
            "Mit --apply werden die Änderungen ausgeführt."
        )


if __name__ == "__main__":
    main()
