"""CSV Exporter module for generating and opening analysis results."""
import csv
import sys
import webbrowser
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from rich.console import Console
from scanner import ZipFileInfo

console = Console(legacy_windows=(sys.platform == "win32"))


class FileRecord:
    """Represents a file record for CSV export."""

    def __init__(
        self,
        original_file: ZipFileInfo,
        file_hash: str,
        is_duplicate: bool,
        duplicate_of: ZipFileInfo | None,
        extracted_date: datetime,
        proposed_location: str
    ):
        self.original_file = original_file
        self.file_hash = file_hash
        self.is_duplicate = is_duplicate
        self.duplicate_of = duplicate_of
        self.extracted_date = extracted_date
        self.proposed_location = proposed_location


def create_csv(
    hash_map: Dict[str, List[ZipFileInfo]],
    file_dates: Dict[ZipFileInfo, datetime],
    proposed_locations: Dict[ZipFileInfo, str],
    output_file: str = "takeout_analysis.csv"
) -> Path:
    """
    Create a CSV file with file analysis results.

    Args:
        hash_map: Hash map from hasher.build_hash_map()
        file_dates: Dictionary mapping ZipFileInfo to their extracted dates
        proposed_locations: Dictionary mapping ZipFileInfo to proposed locations
        output_file: Output CSV filename

    Returns:
        Path to the created CSV file
    """
    records: List[FileRecord] = []

    # Process hash map to create records
    for file_hash, file_infos in hash_map.items():
        if len(file_infos) == 1:
            # Not a duplicate
            file_info = file_infos[0]
            records.append(FileRecord(
                original_file=file_info,
                file_hash=file_hash,
                is_duplicate=False,
                duplicate_of=None,
                extracted_date=file_dates.get(file_info, datetime.now()),
                proposed_location=proposed_locations.get(file_info, "")
            ))
        else:
            # Duplicates - first is original, rest are duplicates
            original = file_infos[0]
            records.append(FileRecord(
                original_file=original,
                file_hash=file_hash,
                is_duplicate=False,
                duplicate_of=None,
                extracted_date=file_dates.get(original, datetime.now()),
                proposed_location=proposed_locations.get(original, "")
            ))

            for duplicate in file_infos[1:]:
                records.append(FileRecord(
                    original_file=duplicate,
                    file_hash=file_hash,
                    is_duplicate=True,
                    duplicate_of=original,
                    extracted_date=file_dates.get(duplicate, datetime.now()),
                    proposed_location=proposed_locations.get(duplicate, "")
                ))

    # Write CSV file
    output_path = Path(output_file)
    console.print(f"[cyan]Writing CSV to: {output_path.absolute()}[/cyan]")

    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow([
            "Zip File",
            "File Path in Zip",
            "File Hash",
            "Is Duplicate",
            "Duplicate Of",
            "Extracted Date",
            "Proposed Location"
        ])

        # Write records
        for record in records:
            writer.writerow([
                str(record.original_file.zip_path.name),
                record.original_file.file_path,
                record.file_hash,
                "Yes" if record.is_duplicate else "No",
                record.duplicate_of.get_display_path() if record.duplicate_of else "",
                record.extracted_date.strftime("%Y-%m-%d %H:%M:%S"),
                record.proposed_location
            ])

    console.print(f"[green]CSV file created with {len(records)} entries[/green]")
    return output_path


def open_csv(csv_path: Path) -> None:
    """
    Open the CSV file with the default application.

    Args:
        csv_path: Path to the CSV file
    """
    try:
        webbrowser.open(csv_path.absolute().as_uri())
        console.print(f"[green]Opening CSV file...[/green]")
    except Exception as e:
        console.print(f"[yellow]Could not open CSV automatically: {e}[/yellow]")
        console.print(f"[yellow]Please open manually: {csv_path.absolute()}[/yellow]")
