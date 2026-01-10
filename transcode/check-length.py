#!/usr/bin/env python3
"""
Prüft Quellen und transkodierte Ziele auf vergleichbare Länge.
- Lädt ENV optional aus --env-file (Default: /etc/transcode-mqtt.env)
- Vergleicht Dauer via ffprobe; Toleranz: max(1s, 1% der Eingangsdauer)
- Meldet fehlende Quellen/Ziele und Dauerabweichungen.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple

TEMP_MKV_RE = re.compile(r"^[A-Za-z0-9]{2}_[A-Za-z][0-9]{2}\.mkv$", re.IGNORECASE)


def getenv(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    with path.open() as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            os.environ[key] = value.strip()


def is_temp_mkv(path: Path) -> bool:
    return bool(TEMP_MKV_RE.match(path.name))


def probe_duration(path: Path) -> Optional[float]:
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
    except Exception:
        return None


def collect_pairs(
    series_src_base: Path,
    series_dst_base: Path,
    movie_src_base: Path,
    movie_dst_base: Path,
) -> Dict[Tuple[str, Path], dict]:
    """
    Liefert Mapping key -> {src: Path|None, dst: Path|None}
    key: ("series"/"movie", relative_path)
    """
    pairs: Dict[Tuple[str, Path], dict] = {}

    # Quellen scannen
    for base, kind, dst_base in [
        (series_src_base, "series", series_dst_base),
        (movie_src_base, "movie", movie_dst_base),
    ]:
        if not base.exists():
            continue
        for mkv in base.rglob("*.mkv"):
            if is_temp_mkv(mkv):
                continue
            try:
                rel = mkv.relative_to(base)
            except ValueError:
                continue
            key = (kind, rel)
            pairs[key] = {"src": mkv, "dst": dst_base / rel}

    # Ziele scannen, um Outputs ohne Quelle zu finden
    for base, kind, src_base in [
        (series_dst_base, "series", series_src_base),
        (movie_dst_base, "movie", movie_src_base),
    ]:
        if not base.exists():
            continue
        for mkv in base.rglob("*.mkv"):
            try:
                rel = mkv.relative_to(base)
            except ValueError:
                continue
            key = (kind, rel)
            entry = pairs.setdefault(key, {})
            entry.setdefault("dst", mkv)
            if "src" not in entry:
                entry["src"] = src_base / rel if (src_base / rel).exists() else None

    return pairs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Vergleicht Dauer von Quell-MKVs und transkodierten Zielen."
    )
    parser.add_argument(
        "--env-file",
        default="/etc/transcode-mqtt.env",
        help="Pfad zu KEY=VALUE Env-Datei (setzt fehlende Variablen)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500,
        help="alle N Einträge einen Fortschritts-Log ausgeben (0=aus)",
    )
    parser.add_argument(
        "--show-missing",
        action="store_true",
        help="fehlende Quellen/Ziele ausgeben (standard: unterdrückt)",
    )
    args = parser.parse_args()

    load_env_file(Path(args.env_file))

    src_base = Path(getenv("SRC_BASE", required=True)).expanduser().resolve()

    series_subpath = Path(getenv("SERIES_SUBPATH", "Serien"))
    if series_subpath.is_absolute():
        raise RuntimeError("SERIES_SUBPATH must be relative")
    series_dst_base = Path(getenv("SERIES_DST_BASE", "/media/Serien")).expanduser().resolve()

    movie_subpath = Path(getenv("MOVIE_SUBPATH", "Filme"))
    if movie_subpath.is_absolute():
        raise RuntimeError("MOVIE_SUBPATH must be relative")
    movie_dst_base = Path(getenv("MOVIE_DST_BASE", "/media/Filme")).expanduser().resolve()

    source_type_default = getenv("SOURCE_TYPE", "dvd").strip().lower()
    if source_type_default not in {"dvd", "bluray"}:
        raise RuntimeError("SOURCE_TYPE must be 'dvd' or 'bluray'")

    source_roots = []
    for candidate in ("dvd", "bluray"):
        candidate_root = src_base / candidate
        if candidate_root.exists():
            source_roots.append(candidate_root)
    if not source_roots:
        source_roots.append(src_base)

    pairs = {}
    for source_root in source_roots:
        series_src_base = (source_root / series_subpath).resolve()
        movie_src_base = (source_root / movie_subpath).resolve()
        pairs.update(
            collect_pairs(
                series_src_base, series_dst_base, movie_src_base, movie_dst_base
            )
        )

    missing_dst = []
    missing_src = []
    mismatches = []
    probe_errors = []

    total = len(pairs)
    if total:
        print(f"Checking {total} entries...")

    for idx, ((kind, rel), entry) in enumerate(sorted(pairs.items()), start=1):
        if args.progress_every and idx % args.progress_every == 0:
            print(f"  progress: {idx}/{total} checked")

        src = entry.get("src")
        dst = entry.get("dst")
        src_exists = src is not None and src.exists()
        dst_exists = dst is not None and dst.exists()

        if not src_exists and dst_exists:
            d_dur = probe_duration(dst) if dst else None
            missing_src.append((kind, rel, dst, d_dur))
            continue
        if src_exists and not dst_exists:
            missing_dst.append((kind, rel, src))
            continue
        if not src_exists and not dst_exists:
            continue

        s_dur = probe_duration(src) if src else None
        d_dur = probe_duration(dst) if dst else None
        if s_dur is None or d_dur is None:
            probe_errors.append((kind, rel, src, dst, s_dur, d_dur))
            continue
        tol = max(1.0, s_dur * 0.01)
        if abs(s_dur - d_dur) > tol:
            mismatches.append((kind, rel, src, dst, s_dur, d_dur, tol))

    print(f"Entries checked: {len(pairs)}")
    if args.show_missing:
        if missing_dst:
            print(f"\nMissing outputs ({len(missing_dst)}):")
            for kind, rel, src in missing_dst:
                print(f"  [{kind}] {src} -> (missing dest)")
        if missing_src:
            print(f"\nMissing sources ({len(missing_src)}):")
            for kind, rel, dst, d_dur in missing_src:
                dur_txt = f"{d_dur:.2f}s" if d_dur is not None else "n/a"
                print(f"  [{kind}] (missing src) -> {dst} (dur {dur_txt})")
    if probe_errors:
        print(f"\nProbe errors ({len(probe_errors)}):")
        for kind, rel, src, dst, s_dur, d_dur in probe_errors:
            print(
                f"  [{kind}] {src} -> {dst} (src dur={s_dur}, dst dur={d_dur})"
            )
    if mismatches:
        print(f"\nDuration mismatches ({len(mismatches)}):")
        for kind, rel, src, dst, s_dur, d_dur, tol in mismatches:
            print(
                f"  [{kind}] {src} -> {dst}: in={s_dur:.2f}s out={d_dur:.2f}s tol={tol:.2f}s"
            )

    if not (missing_dst or missing_src or probe_errors or mismatches):
        print("All OK within tolerance.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
