"""Scanner module for scanning zip files."""
import os
import sys
import zipfile
from pathlib import Path
from dataclasses import dataclass
from typing import List
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Create console with legacy Windows support if needed
console = Console(legacy_windows=(sys.platform == "win32"))


@dataclass(frozen=True)
class ZipFileInfo:
    """Represents a file inside a zip archive."""
    zip_path: Path
    file_path: str
    file_size: int
    file_crc: int  # CRC32 from zip metadata - free duplicate detection!
    date_time: tuple  # (year, month, day, hour, minute, second) from zip metadata

    def get_display_path(self) -> str:
        """Get a human-readable path for display."""
        return f"{self.zip_path.name}:{self.file_path}"

    def __str__(self) -> str:
        return self.get_display_path()

    def get_content_key(self) -> str:
        """Get a key representing file content (size + CRC32). Fast duplicate detection."""
        return f"{self.file_size}_{self.file_crc:08x}"

    def get_zip_date(self) -> "datetime":
        """Get the date from zip metadata as a datetime object."""
        from datetime import datetime
        # 1980 is the DOS minimum date - often used as "no date" placeholder
        # Only accept dates from 1981 onwards as valid
        if self.date_time and self.date_time[0] > 1980:
            return datetime(*self.date_time)
        return None


def scan_directory(root_path: str) -> List[ZipFileInfo]:
    """
    Scan a directory recursively for zip files and return all files found inside them.

    Args:
        root_path: Root directory to scan for zip files (scans all subdirectories)

    Returns:
        List of ZipFileInfo objects for all files found in zip archives
    """
    root = Path(root_path)

    if not root.exists():
        console.print(f"[red]Error: Directory '{root_path}' does not exist[/red]")
        return []

    if not root.is_dir():
        console.print(f"[red]Error: '{root_path}' is not a directory[/red]")
        return []

    # Find all zip files recursively
    zip_files = list(root.glob("**/*.zip"))

    if not zip_files:
        console.print(f"[yellow]Warning: No zip files found in {root_path}[/yellow]")
        return []

    console.print(f"[cyan]Found {len(zip_files)} zip file(s) to scan[/cyan]")

    all_files = []

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Scanning zip files...", total=None)

        for zip_path in zip_files:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for zip_info in zip_ref.infolist():
                        # Skip directories
                        if not zip_info.is_dir():
                            file_info = ZipFileInfo(
                                zip_path=zip_path,
                                file_path=zip_info.filename,
                                file_size=zip_info.file_size,
                                file_crc=zip_info.CRC,
                                date_time=zip_info.date_time
                            )
                            all_files.append(file_info)
                            progress.update(
                                task,
                                description=f"Scanning zip files... ({len(all_files)} files found)"
                            )
            except zipfile.BadZipFile:
                console.print(f"[yellow]Warning: {zip_path.name} is not a valid zip file[/yellow]")
            except Exception as e:
                console.print(f"[yellow]Warning: Error reading {zip_path.name}: {e}[/yellow]")

    console.print(f"[green]Found {len(all_files)} files in {len(zip_files)} zip archive(s)[/green]")
    return all_files
