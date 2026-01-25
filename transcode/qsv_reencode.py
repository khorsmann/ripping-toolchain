#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

JELLYFIN_FFMPEG = "/usr/lib/jellyfin-ffmpeg/ffmpeg"
JELLYFIN_FFPROBE = "/usr/lib/jellyfin-ffmpeg/ffprobe"
RENDER_NODE = "/dev/dri/renderD128"


def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def ffprobe_field_order(ffprobe_bin: str, infile: Path) -> str:
    cmd = [
        ffprobe_bin,
        "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=field_order",
        "-of", "csv=p=0",
        str(infile),
    ]
    p = run(cmd)
    if p.returncode != 0:
        # If ffprobe fails, be conservative and return "unknown"
        return "unknown"
    return (p.stdout or "").strip() or "unknown"


def is_interlaced(field_order: str) -> bool:
    # Typical values: progressive, unknown, tb, bt, tt, bb
    # Anything other than progressive/unknown we'll treat as interlaced.
    fo = field_order.lower()
    return fo not in ("progressive", "unknown", "")


def build_ffmpeg_cmd(ffmpeg_bin: str, infile: Path, outfile: Path, global_quality: int) -> list[str]:
    # QSV init via VAAPI -> QSV (works in your environment)
    hw_init = [
        "-init_hw_device", f"vaapi=va:{RENDER_NODE}",
        "-init_hw_device", "qsv=qsv@va",
        "-filter_hw_device", "qsv",
    ]

    # 25p deinterlace, then upload for QSV encode
    vf = "bwdif=mode=send_frame:parity=auto:deint=all,format=nv12,hwupload=extra_hw_frames=64"

    return [
        ffmpeg_bin,
        "-hide_banner",
        "-y",
        *hw_init,
        "-i", str(infile),
        "-map", "0",
        "-map_metadata", "0",
        "-map_chapters", "0",
        "-vf", vf,
        "-c:v", "hevc_qsv",
        "-preset", "medium",
        "-global_quality", str(global_quality),
        "-look_ahead", "1",
        "-c:a", "copy",
        "-c:s", "copy",
        str(outfile),
    ]


def atomic_replace(src_tmp: Path, dst_final: Path, keep_backup: bool) -> None:
    # Ensure same directory for atomic rename
    if src_tmp.parent != dst_final.parent:
        raise RuntimeError("Temp file must be in the same directory as destination for atomic replace")

    backup = dst_final.with_suffix(dst_final.suffix + ".bak")

    if keep_backup:
        if backup.exists():
            backup.unlink()
        dst_final.rename(backup)
        src_tmp.rename(dst_final)
    else:
        # os.replace is atomic on same filesystem
        os.replace(src_tmp, dst_final)


def process_file(ffmpeg_bin: str, ffprobe_bin: str, infile: Path, global_quality: int,
                 dry_run: bool, keep_backup: bool) -> bool:
    field_order = ffprobe_field_order(ffprobe_bin, infile)
    if not is_interlaced(field_order):
        print(f"SKIP (not interlaced): {infile} [field_order={field_order}]")
        return False

    print(f"PROC: {infile} [field_order={field_order}]")

    # Create temp file in same directory for atomic replace
    tmp_name = infile.name + ".tmp_transcode.mkv"
    tmp_path = infile.with_name(tmp_name)

    if tmp_path.exists():
        tmp_path.unlink()

    cmd = build_ffmpeg_cmd(ffmpeg_bin, infile, tmp_path, global_quality)

    if dry_run:
        print("DRY-RUN CMD:", " ".join(cmd))
        return True

    p = run(cmd)
    if p.returncode != 0:
        print(f"ERROR: ffmpeg failed for {infile}")
        print(p.stderr.strip())
        if tmp_path.exists():
            tmp_path.unlink()
        return False

    # Basic sanity check: non-empty file
    if not tmp_path.exists() or tmp_path.stat().st_size < 1024 * 1024:
        print(f"ERROR: output too small / missing: {tmp_path}")
        if tmp_path.exists():
            tmp_path.unlink()
        return False

    # Preserve permissions/ownership where possible
    try:
        st = infile.stat()
        os.chmod(tmp_path, st.st_mode)
        try:
            os.chown(tmp_path, st.st_uid, st.st_gid)
        except PermissionError:
            pass
    except FileNotFoundError:
        print(f"ERROR: input vanished: {infile}")
        if tmp_path.exists():
            tmp_path.unlink()
        return False

    # Replace original atomically
    atomic_replace(tmp_path, infile, keep_backup=keep_backup)
    print(f"DONE: replaced {infile}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="Recursive QSV deinterlace+transcode (Voyager DVD PAL friendly), then replace originals.")
    ap.add_argument("root", type=Path, help="Root folder to scan recursively")
    ap.add_argument("--ffmpeg", default=JELLYFIN_FFMPEG, help="Path to jellyfin ffmpeg")
    ap.add_argument("--ffprobe", default=JELLYFIN_FFPROBE, help="Path to jellyfin ffprobe")
    ap.add_argument("--global-quality", type=int, default=16,
                    help="QSV global_quality (lower=better/larger). Typical SD: 14-20. Default 16.")
    ap.add_argument("--ext", default=".mkv", help="File extension filter (default .mkv)")
    ap.add_argument("--dry-run", action="store_true", help="Print actions/commands, do not encode/replace")
    ap.add_argument("--keep-backup", action="store_true", help="Keep a .bak of the original file")
    args = ap.parse_args()

    root = args.root
    if not root.exists():
        print(f"Root does not exist: {root}", file=sys.stderr)
        return 2

    # Basic binary checks
    for b in (args.ffmpeg, args.ffprobe):
        if not Path(b).exists():
            print(f"Missing binary: {b}", file=sys.stderr)
            return 2

    ext = args.ext.lower()
    processed = 0
    changed = 0

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() != ext:
            continue
        # Avoid re-processing temp files or backups
        if path.name.endswith(".tmp_transcode.mkv") or path.name.endswith(".bak"):
            continue

        processed += 1
        ok = process_file(args.ffmpeg, args.ffprobe, path, args.global_quality, args.dry_run, args.keep_backup)
        if ok:
            changed += 1

    print(f"\nScanned: {processed} files, processed: {changed} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
