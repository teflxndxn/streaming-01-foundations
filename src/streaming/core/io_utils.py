"""CSV input/output helpers for streaming examples."""

import csv
from pathlib import Path
from typing import Any


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV file into a list of string dictionaries."""
    with path.open(mode="r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return list(reader)


def append_csv_row(path: Path, row: dict[str, Any], fieldnames: list[str]) -> None:
    """Append one row to a CSV file, writing the header first if needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()

    with path.open(mode="a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)
