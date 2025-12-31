#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path
import argparse
import sys

KEEP_LANGS = {"eng", "deu", "ger"}


def base_without_clean(path: Path) -> Path:
    """Entfernt ein trailing _clean vor .mkv"""
    if path.stem.endswith("_clean"):
        return path.with_name(path.stem[:-6] + ".mkv")
    return path


def process_file(mkv: Path, dry_run=False):
    # bereits bereinigte Dateien überspringen
    if mkv.stem.endswith("_clean"):
        print(f"⏭  Übersprungen (bereits clean): {mkv.name}")
        return

    out_file = mkv.with_name(mkv.stem + "_clean.mkv")

    if out_file.exists():
        print(f"⏭  Clean-Datei existiert schon: {out_file.name}")
        return

    print(f"Verarbeite: {mkv.name}")

    probe = subprocess.run(
        ["mkvmerge", "-J", str(mkv)], capture_output=True, text=True, check=True
    )
    info = json.loads(probe.stdout)

    video_ids, audio_ids, sub_ids = [], [], []

    for t in info["tracks"]:
        tid = str(t["id"])
        lang = t.get("properties", {}).get("language", "").lower()

        if t["type"] == "video":
            video_ids.append(tid)

        elif t["type"] == "audio" and lang in KEEP_LANGS:
            audio_ids.append(tid)

        elif t["type"] == "subtitles" and lang in KEEP_LANGS:
            sub_ids.append(tid)

    if not video_ids:
        print("⚠️  Keine Videospur – übersprungen\n")
        return

    cmd = [
        "mkvmerge",
        "-o",
        str(out_file),
        "-d",
        ",".join(video_ids),
        "-a",
        ",".join(audio_ids) if audio_ids else "",
        "-s",
        ",".join(sub_ids) if sub_ids else "",
        str(mkv),
    ]
    cmd = [c for c in cmd if c]

    print("→", " ".join(cmd))

    if not dry_run:
        subprocess.run(cmd, check=True)

    print(f"✔ Fertig: {out_file.name}\n")


def promote_clean(path: Path, dry_run=False):
    """
    Macht:
      file_clean.mkv → file.mkv
      vorhandene file.mkv wird überschrieben
    """
    for clean in sorted(path.glob("*_clean.mkv")):
        target = base_without_clean(clean)

        # Falls schon clean umbenannt wurde → nichts tun
        if clean == target:
            print(f"⏭ Bereits final: {clean.name}")
            continue

        print(f"⤴ Promote: {clean.name} → {target.name}")

        if target.exists():
            print(f"⚠️  Überschreibe bestehende Datei: {target.name}")

        if not dry_run:
            # Erst neue Datei platzieren, dann alte überschreiben
            clean.replace(target)


def iter_targets(path: Path):
    if path.is_file():
        if path.suffix.lower() == ".mkv":
            yield path
        else:
            print(f"⚠️  Ignoriert (kein .mkv): {path}")
    elif path.is_dir():
        for mkv in sorted(path.glob("*.mkv")):
            yield mkv
    else:
        print(f"❌ Pfad existiert nicht: {path}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="MKV-Spuren bereinigen (nur Deutsch & Englisch behalten)"
    )

    parser.add_argument("path", help="Datei oder Ordner")

    parser.add_argument(
        "--promote",
        action="store_true",
        help="*_clean.mkv zu *.mkv verschieben (Original → *.bak)",
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Nur anzeigen, nichts ausführen"
    )

    args = parser.parse_args()
    path = Path(args.path)

    if args.promote:
        promote_clean(path if path.is_dir() else path.parent, dry_run=args.dry_run)
        return

    for mkv in iter_targets(path):
        process_file(mkv, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
