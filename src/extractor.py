"""Extractor module for extracting files from zip archives."""
import sys
import shutil
import zipfile
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple, Optional, Callable
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from scanner import ZipFileInfo

console = Console(legacy_windows=(sys.platform == "win32"))


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


@dataclass
class ExtractionResult:
    """Result of extracting a single file."""
    file_info: ZipFileInfo
    destination: Path
    success: bool
    error: Optional[str] = None
    skipped: bool = False
    bytes_extracted: int = 0


@dataclass
class ZipExtractionSummary:
    """Summary of extracting all files from a zip."""
    zip_path: Path
    total_files: int
    extracted_count: int
    skipped_count: int
    error_count: int
    bytes_extracted: int
    errors: List[str]


class FileExtractor:
    """Handles extracting files from zip archives with collision handling."""

    def __init__(self, base_dir: Path):
        """
        Initialize the extractor.

        Args:
            base_dir: Base directory for all extractions
        """
        self.base_dir = Path(base_dir)

    def resolve_collision(self, destination: Path) -> Path:
        """
        Handle filename collisions by appending _1, _2, etc.

        Args:
            destination: Original destination path

        Returns:
            A path that doesn't exist (either original or with suffix)
        """
        if not destination.exists():
            return destination

        stem = destination.stem
        suffix = destination.suffix
        parent = destination.parent

        counter = 1
        while True:
            new_name = f"{stem}_{counter}{suffix}"
            new_path = parent / new_name
            if not new_path.exists():
                return new_path
            counter += 1
            if counter > 10000:  # Safety limit
                raise RuntimeError(f"Too many collisions for {destination}")

    def extract_file(
        self,
        zip_ref: zipfile.ZipFile,
        file_info: ZipFileInfo,
        destination: Path
    ) -> ExtractionResult:
        """
        Extract a single file from a zip archive.

        Args:
            zip_ref: Open ZipFile reference
            file_info: Information about the file to extract
            destination: Where to extract the file

        Returns:
            ExtractionResult with success/failure info
        """
        try:
            # Ensure parent directory exists
            destination.parent.mkdir(parents=True, exist_ok=True)

            # Handle collision
            final_dest = self.resolve_collision(destination)

            # Extract file
            with zip_ref.open(file_info.file_path) as src:
                with open(final_dest, 'wb') as dst:
                    shutil.copyfileobj(src, dst)

            return ExtractionResult(
                file_info=file_info,
                destination=final_dest,
                success=True,
                bytes_extracted=file_info.file_size
            )

        except PermissionError as e:
            return ExtractionResult(
                file_info=file_info,
                destination=destination,
                success=False,
                error=f"Permission denied: {e}"
            )
        except OSError as e:
            return ExtractionResult(
                file_info=file_info,
                destination=destination,
                success=False,
                error=str(e)
            )
        except Exception as e:
            return ExtractionResult(
                file_info=file_info,
                destination=destination,
                success=False,
                error=str(e)
            )

    def extract_unique_files(
        self,
        zip_path: Path,
        files_to_extract: List[Tuple[ZipFileInfo, Path]],
        progress_callback: Optional[Callable[[ZipFileInfo, bool, Optional[str]], None]] = None
    ) -> ZipExtractionSummary:
        """
        Extract unique files from a zip archive.

        Args:
            zip_path: Path to the zip file
            files_to_extract: List of (ZipFileInfo, destination_path) tuples
            progress_callback: Optional callback(file_info, success, error_msg)

        Returns:
            ZipExtractionSummary with extraction statistics
        """
        extracted_count = 0
        skipped_count = 0
        error_count = 0
        bytes_extracted = 0
        errors: List[str] = []

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info, proposed_dest in files_to_extract:
                    result = self.extract_file(zip_ref, file_info, proposed_dest)

                    if result.success:
                        extracted_count += 1
                        bytes_extracted += result.bytes_extracted
                    elif result.skipped:
                        skipped_count += 1
                    else:
                        error_count += 1
                        errors.append(f"{file_info.file_path}: {result.error}")

                    if progress_callback:
                        progress_callback(file_info, result.success, result.error)

        except zipfile.BadZipFile as e:
            errors.append(f"Bad zip file: {e}")
            error_count = len(files_to_extract)

        return ZipExtractionSummary(
            zip_path=zip_path,
            total_files=len(files_to_extract),
            extracted_count=extracted_count,
            skipped_count=skipped_count,
            error_count=error_count,
            bytes_extracted=bytes_extracted,
            errors=errors
        )


def extract_all_unique(
    files_by_zip: dict,
    progress_console: Console = None
) -> Tuple[int, int, int]:
    """
    Extract all unique files from multiple zips.

    Args:
        files_by_zip: Dict[Path, List[Tuple[ZipFileInfo, Path]]] - files grouped by zip
        progress_console: Console for progress output

    Returns:
        Tuple of (total_extracted, total_errors, total_bytes)
    """
    if progress_console is None:
        progress_console = console

    # Handle empty input
    if not files_by_zip:
        return 0, 0, 0

    total_extracted = 0
    total_errors = 0
    total_bytes = 0

    # Get base_dir from first file's destination
    first_zip = next(iter(files_by_zip.values()))
    if first_zip:
        base_dir = Path(first_zip[0][1]).parts[0]
    else:
        base_dir = "extracted"

    extractor = FileExtractor(base_dir=Path(base_dir))

    sorted_zips = sorted(files_by_zip.keys())

    for zip_path in sorted_zips:
        files_to_extract = files_by_zip[zip_path]
        file_count = len(files_to_extract)
        total_size = sum(f.file_size for f, _ in files_to_extract)

        progress_console.print(
            f"\n[bold cyan]Extracting from:[/bold cyan] {zip_path.name} "
            f"({file_count} files, {format_size(total_size)})"
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            console=progress_console
        ) as progress:
            task = progress.add_task("Extracting...", total=file_count)
            extracted_in_zip = 0

            def update_progress(file_info: ZipFileInfo, success: bool, error: Optional[str]):
                nonlocal extracted_in_zip
                if success:
                    extracted_in_zip += 1
                progress.update(
                    task,
                    advance=1,
                    description=f"Extracting... ({extracted_in_zip}/{file_count})"
                )

            summary = extractor.extract_unique_files(
                zip_path,
                files_to_extract,
                update_progress
            )

        total_extracted += summary.extracted_count
        total_errors += summary.error_count
        total_bytes += summary.bytes_extracted

        # Show summary for this zip
        if summary.error_count > 0:
            progress_console.print(
                f"  [green]{summary.extracted_count} extracted[/green], "
                f"[red]{summary.error_count} errors[/red]"
            )
            for err in summary.errors[:3]:  # Show first 3 errors
                progress_console.print(f"    [dim red]{err}[/dim red]")
            if len(summary.errors) > 3:
                progress_console.print(f"    [dim]...and {len(summary.errors) - 3} more errors[/dim]")
        else:
            progress_console.print(f"  [green]{summary.extracted_count} files extracted[/green]")

    return total_extracted, total_errors, total_bytes
