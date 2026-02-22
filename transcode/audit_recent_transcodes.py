#!/usr/bin/env python3
"""
Prueft transkodierte MKVs aus den letzten N Tagen gegen vorhandenes Rohmaterial.

Die Pfad- und Struktur-Logik folgt transcode/rescan.py:
- Serien:   SRC_BASE/<source_type>/<SERIES_SUBPATH>/... -> SERIES_DST_BASE/...
- Filme:    SRC_BASE/<source_type>/<MOVIE_SUBPATH>/...  -> MOVIE_DST_BASE/...
            (inkl. Legacy-Zielpfad mit Unterordnern)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Sequence, Tuple

TEMP_MKV_RE = re.compile(r"^[A-Za-z0-9]{2}_[A-Za-z][0-9]{2}\.mkv$", re.IGNORECASE)


def getenv(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    if val is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return val


def parse_source_type(value: str) -> str | None:
    cleaned = (value or "").strip().lower()
    if cleaned in {"dvd", "bluray"}:
        return cleaned
    return None


def collect_source_roots(src_base: Path, default_type: str) -> List[Tuple[str, Path]]:
    roots: List[Tuple[str, Path]] = []
    for candidate in ("dvd", "bluray"):
        path = src_base / candidate
        if path.exists():
            roots.append((candidate, path))
    if roots:
        return roots
    return [(default_type, src_base)]


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


def collect_recent_mkvs(base: Path, cutoff_ts: float) -> List[Path]:
    if not base.exists():
        return []
    recent: List[Path] = []
    for mkv in base.rglob("*.mkv"):
        if mkv.is_file() and mkv.stat().st_mtime >= cutoff_ts:
            recent.append(mkv)
    return sorted(recent)


def first_existing(paths: Sequence[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Findet transkodierte MKVs aus den letzten Tagen und prueft, "
            "ob passendes Rohmaterial existiert."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="Zeitraum in Tagen rueckwirkend ab jetzt (Default: 3)",
    )
    parser.add_argument(
        "--env-file",
        default="/etc/transcode-mqtt.env",
        help="Pfad zu einer KEY=VALUE Env-Datei (Default: /etc/transcode-mqtt.env)",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Nur fehlende Raw-Zuordnungen ausgeben",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Loescht vorhandene Transcodes mit RAW-Match (nur OK-Faelle)",
    )
    return parser.parse_args()


def format_candidates(candidates: Sequence[Path]) -> str:
    if not candidates:
        return "-"
    uniq: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        uniq.append(candidate)
    return ", ".join(str(p) for p in uniq)


def safe_to_delete(path: Path, allowed_bases: Sequence[Path]) -> tuple[bool, str]:
    if path.suffix.lower() != ".mkv":
        return False, "not an mkv file"
    if not path.exists() or not path.is_file():
        return False, "file missing or not a regular file"
    if path.is_symlink():
        return False, "refuse to delete symlink"
    resolved = path.resolve()
    if not any(resolved.is_relative_to(base) for base in allowed_bases):
        return False, "path outside allowed destination bases"
    return True, ""


def main() -> int:
    args = parse_args()
    if args.days < 0:
        print("--days muss >= 0 sein", file=sys.stderr)
        return 2

    load_env_file(Path(args.env_file))

    src_base = Path(getenv("SRC_BASE", required=True)).expanduser().resolve()
    source_type_default = getenv("SOURCE_TYPE", "dvd").strip().lower()
    if source_type_default not in {"dvd", "bluray"}:
        raise RuntimeError("SOURCE_TYPE must be 'dvd' or 'bluray'")

    series_subpath = Path(getenv("SERIES_SUBPATH", "Serien"))
    if series_subpath.is_absolute():
        raise RuntimeError("SERIES_SUBPATH must be relative")
    movie_subpath = Path(getenv("MOVIE_SUBPATH", "Filme"))
    if movie_subpath.is_absolute():
        raise RuntimeError("MOVIE_SUBPATH must be relative")

    series_dst_base = Path(getenv("SERIES_DST_BASE", "/media/Serien")).expanduser().resolve()
    movie_dst_base = Path(getenv("MOVIE_DST_BASE", "/media/Filme")).expanduser().resolve()

    source_roots = collect_source_roots(src_base, source_type_default)

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    cutoff_ts = cutoff.timestamp()

    series_recent = collect_recent_mkvs(series_dst_base, cutoff_ts)
    movie_recent = collect_recent_mkvs(movie_dst_base, cutoff_ts)
    allowed_delete_bases = (series_dst_base, movie_dst_base)

    missing = 0
    ok = 0
    scanned = 0
    deleted_ok = 0
    delete_failed = 0

    for transcoded in series_recent:
        scanned += 1
        rel = transcoded.relative_to(series_dst_base)
        candidates = [root / series_subpath / rel for _, root in source_roots]
        raw_match = first_existing(candidates)
        if raw_match:
            ok += 1
            if args.delete:
                can_delete, reason = safe_to_delete(transcoded, allowed_delete_bases)
                if not can_delete:
                    delete_failed += 1
                    print(
                        f"DELETE-ERR | {transcoded} | safety check failed: {reason} | raw: {raw_match}"
                    )
                    continue
                try:
                    transcoded.unlink()
                    deleted_ok += 1
                    print(f"DELETED | {transcoded} | raw: {raw_match}")
                except OSError as err:
                    delete_failed += 1
                    print(f"DELETE-ERR | {transcoded} | {err} | raw: {raw_match}")
                continue
            if args.only_missing:
                continue
            print(f"OK      | {transcoded} | raw: {raw_match}")
            continue

        missing += 1
        print(f"MISSING | {transcoded} | raw? {format_candidates(candidates)}")

    for transcoded in movie_recent:
        scanned += 1
        rel = transcoded.relative_to(movie_dst_base)
        basename_rel = Path(rel.name)
        candidates = []
        for _, root in source_roots:
            movie_src_base = (root / movie_subpath).resolve()
            candidates.append(movie_src_base / rel)
            candidates.append(movie_src_base / basename_rel)

        raw_match = first_existing(candidates)
        if raw_match:
            ok += 1
            if args.delete:
                can_delete, reason = safe_to_delete(transcoded, allowed_delete_bases)
                if not can_delete:
                    delete_failed += 1
                    print(
                        f"DELETE-ERR | {transcoded} | safety check failed: {reason} | raw: {raw_match}"
                    )
                    continue
                try:
                    transcoded.unlink()
                    deleted_ok += 1
                    print(f"DELETED | {transcoded} | raw: {raw_match}")
                except OSError as err:
                    delete_failed += 1
                    print(f"DELETE-ERR | {transcoded} | {err} | raw: {raw_match}")
                continue
            if args.only_missing:
                continue
            print(f"OK      | {transcoded} | raw: {raw_match}")
            continue

        missing += 1
        print(f"MISSING | {transcoded} | raw? {format_candidates(candidates)}")

    print("")
    print(f"cutoff_utc={cutoff.isoformat()}")
    print(f"series_recent={len(series_recent)} movie_recent={len(movie_recent)} scanned={scanned}")
    print(
        f"ok={ok} missing={missing} deleted_ok={deleted_ok} delete_failed={delete_failed}"
    )
    if args.delete:
        return 1 if delete_failed else 0
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
