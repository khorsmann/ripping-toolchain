#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prueft, welche Dateien aus einer ReportExport-CSV auf dem Dateisystem "
            "nicht vorhanden sind, gibt sie aus und schreibt sie in eine neue CSV."
        )
    )
    parser.add_argument(
        "-i",
        "--input",
        default="ReportExport.csv",
        help="Pfad zur Eingabe-CSV (Standard: ReportExport.csv)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="MissingFromReport.csv",
        help="Pfad zur Ausgabe-CSV fuer fehlende Dateien (Standard: MissingFromReport.csv)",
    )
    return parser.parse_args()


def find_missing_entries(input_csv: Path) -> list[tuple[str, str]]:
    missing: list[tuple[str, str]] = []

    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV hat keine Kopfzeile.")

        required = {"Name", "Path"}
        missing_cols = required - set(reader.fieldnames)
        if missing_cols:
            cols = ", ".join(sorted(missing_cols))
            raise ValueError(f"CSV enthaelt erforderliche Spalten nicht: {cols}")

        for row in reader:
            name = (row.get("Name") or "").strip()
            path_str = (row.get("Path") or "").strip()
            if not path_str:
                continue

            if not Path(path_str).exists():
                missing.append((name, path_str))

    return missing


def write_missing_csv(output_csv: Path, rows: list[tuple[str, str]]) -> None:
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Path"])
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input)
    output_csv = Path(args.output)

    if not input_csv.exists():
        print(f"Eingabe-CSV nicht gefunden: {input_csv}")
        return 1

    try:
        missing = find_missing_entries(input_csv)
    except ValueError as err:
        print(f"Fehler: {err}")
        return 1

    for name, path_str in missing:
        print(f"{name} | {path_str}")

    write_missing_csv(output_csv, missing)
    print(f"\nFehlende Dateien: {len(missing)}")
    print(f"CSV geschrieben: {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
