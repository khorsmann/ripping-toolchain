#!/usr/bin/env python3
"""
Find duplicate file names recursively.

Usage:
  python transcode/find_duplicate_filenames.py [start_path]
"""

from __future__ import annotations

import argparse
import fnmatch
import os
from collections import defaultdict
from pathlib import Path


def matches_glob(name: str, pattern: str, ignore_case: bool) -> bool:
    if ignore_case:
        return fnmatch.fnmatch(name.lower(), pattern.lower())
    return fnmatch.fnmatchcase(name, pattern)


def find_duplicate_filenames(
    start_path: Path, pattern: str, ignore_case: bool, use_stem: bool
) -> dict[str, list[Path]]:
    files_by_name: dict[str, list[Path]] = defaultdict(list)

    for root, _, files in os.walk(start_path):
        root_path = Path(root)
        for filename in files:
            if not matches_glob(filename, pattern, ignore_case):
                continue
            key = Path(filename).stem if use_stem else filename
            files_by_name[key].append(root_path / filename)

    return {name: paths for name, paths in files_by_name.items() if len(paths) > 1}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search recursively for duplicate file names."
    )
    parser.add_argument(
        "start_path",
        nargs="?",
        default=".",
        help="Directory to search (default: current directory).",
    )
    parser.add_argument(
        "--glob",
        default="*",
        help="Only include files matching this glob pattern (default: '*'). Example: '*.mkv'",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Make glob matching case-sensitive (default: case-insensitive).",
    )
    parser.add_argument(
        "--stem",
        action="store_true",
        help="Compare by file name without extension (e.g. a.mkv and a.mp4 count as duplicate).",
    )
    args = parser.parse_args()

    start_path = Path(args.start_path).resolve()
    if not start_path.exists():
        print(f"Path does not exist: {start_path}")
        raise SystemExit(1)
    if not start_path.is_dir():
        print(f"Not a directory: {start_path}")
        raise SystemExit(1)

    duplicates = find_duplicate_filenames(
        start_path=start_path,
        pattern=args.glob,
        ignore_case=not args.case_sensitive,
        use_stem=args.stem,
    )
    if not duplicates:
        print(f"No duplicate file names found in: {start_path}")
        return

    print(f"Duplicate file names in: {start_path}\n")
    for filename in sorted(duplicates):
        print(f"{filename} ({len(duplicates[filename])}x)")
        for path in sorted(duplicates[filename]):
            print(f"  - {path}")
        print()


if __name__ == "__main__":
    main()
