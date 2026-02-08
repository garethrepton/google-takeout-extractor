"""Comparator module for comparing zip files against directory structures."""
import sys
import io
import zlib
import hashlib
import zipfile
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from scanner import ZipFileInfo
from directory_scanner import DirectoryScanner, DirectoryFileInfo

console = Console(legacy_windows=(sys.platform == "win32"))


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


@dataclass
class ComparisonResult:
    """Result of comparing a zip file against a directory."""
    zip_path: Path
    directory: Path
    duplicates: List[Tuple[ZipFileInfo, DirectoryFileInfo]]  # (zip_file, matching_dir_file)
    unique_in_zip: List[ZipFileInfo]
    unique_in_dir: List[DirectoryFileInfo]
    total_zip_files: int
    total_dir_files: int

    @property
    def duplicate_size(self) -> int:
        """Total size of duplicate files in the zip."""
        return sum(z.file_size for z, _ in self.duplicates)

    @property
    def unique_size(self) -> int:
        """Total size of unique files in the zip."""
        return sum(z.file_size for z in self.unique_in_zip)


class ZipDirectoryComparator:
    """Compares zip file contents against a directory structure."""

    def __init__(self, hash_strategy: str = "size_partial"):
        """
        Initialize the comparator.

        Args:
            hash_strategy: Hashing strategy for directory files
                - "size_partial": Fast partial hash (recommended)
                - "size_crc": CRC32 (slower but matches zip exactly)
                - "full": Full SHA256 (slowest)
        """
        self.hash_strategy = hash_strategy
        self.dir_scanner = DirectoryScanner(hash_strategy)

    def compare(
        self,
        zip_path: Path,
        directory: Path
    ) -> ComparisonResult:
        """
        Compare a zip file's contents against a directory.

        Args:
            zip_path: Path to the zip file
            directory: Path to the directory to compare against

        Returns:
            ComparisonResult with duplicate and unique file lists
        """
        zip_path = Path(zip_path)
        directory = Path(directory)

        # Step 1: Scan directory and build hash map
        console.print(f"\n[bold cyan]Phase 1:[/bold cyan] Scanning directory: {directory}")
        dir_files = self.dir_scanner.scan_directory(directory)
        dir_hash_map: Dict[str, List[DirectoryFileInfo]] = defaultdict(list)
        for f in dir_files:
            dir_hash_map[f.get_content_key()].append(f)

        # Step 2: Scan zip file and compute comparable hashes
        console.print(f"\n[bold cyan]Phase 2:[/bold cyan] Scanning zip: {zip_path.name}")
        zip_files: List[Tuple[ZipFileInfo, str]] = []

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_infos = [zi for zi in zip_ref.infolist() if not zi.is_dir()]

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=40),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Scanning zip...", total=len(zip_infos))

                for zip_info in zip_infos:
                    # Create ZipFileInfo
                    file_info = ZipFileInfo(
                        zip_path=zip_path,
                        file_path=zip_info.filename,
                        file_size=zip_info.file_size,
                        file_crc=zip_info.CRC,
                        date_time=zip_info.date_time
                    )

                    # Compute hash using same strategy as directory
                    content_key = self._get_zip_content_key(zip_ref, zip_info)
                    zip_files.append((file_info, content_key))

                    progress.update(task, advance=1)

        console.print(f"[green]Found {len(zip_files)} files in zip[/green]")

        # Step 3: Compare
        console.print(f"\n[bold cyan]Phase 3:[/bold cyan] Comparing files...")
        duplicates: List[Tuple[ZipFileInfo, DirectoryFileInfo]] = []
        unique_in_zip: List[ZipFileInfo] = []

        for zip_file, content_key in zip_files:
            if content_key in dir_hash_map:
                # Found duplicate
                duplicates.append((zip_file, dir_hash_map[content_key][0]))
            else:
                unique_in_zip.append(zip_file)

        # Find files unique to directory (not in zip)
        zip_keys = {key for _, key in zip_files}
        unique_in_dir = [f for f in dir_files if f.get_content_key() not in zip_keys]

        return ComparisonResult(
            zip_path=zip_path,
            directory=directory,
            duplicates=duplicates,
            unique_in_zip=unique_in_zip,
            unique_in_dir=unique_in_dir,
            total_zip_files=len(zip_files),
            total_dir_files=len(dir_files)
        )

    def _get_zip_content_key(
        self,
        zip_ref: zipfile.ZipFile,
        zip_info: zipfile.ZipInfo
    ) -> str:
        """
        Get content key for a zip entry using the same strategy as directory files.

        Args:
            zip_ref: Open ZipFile reference
            zip_info: ZipInfo for the file

        Returns:
            Content key string for comparison
        """
        file_size = zip_info.file_size

        if self.hash_strategy == "size_crc":
            # Use native zip CRC - fastest, no content reading needed
            return f"{file_size}_{zip_info.CRC:08x}"

        elif self.hash_strategy == "size_partial":
            # Read and compute partial hash
            return self._compute_partial_hash_from_zip(zip_ref, zip_info)

        else:  # full
            # Compute full SHA256
            with zip_ref.open(zip_info) as f:
                content = f.read()
                return f"{len(content)}_{hashlib.sha256(content).hexdigest()}"

    def _compute_partial_hash_from_zip(
        self,
        zip_ref: zipfile.ZipFile,
        zip_info: zipfile.ZipInfo,
        sample_size: int = 64 * 1024
    ) -> str:
        """
        Compute partial hash from zip entry matching directory strategy.

        Args:
            zip_ref: Open ZipFile reference
            zip_info: ZipInfo for the file
            sample_size: Size of each sample chunk

        Returns:
            Content key: "{file_size}_{partial_hash}"
        """
        file_size = zip_info.file_size
        hasher = hashlib.sha256()

        with zip_ref.open(zip_info) as f:
            content = f.read()

        # First chunk
        hasher.update(content[:sample_size])

        if file_size > sample_size * 2:
            # Middle chunk
            middle_start = file_size // 2
            hasher.update(content[middle_start:middle_start + sample_size])

        if file_size > sample_size:
            # Last chunk
            hasher.update(content[-sample_size:])

        return f"{file_size}_{hasher.hexdigest()[:16]}"

    def print_summary(self, result: ComparisonResult) -> None:
        """
        Print a summary of comparison results.

        Args:
            result: ComparisonResult to summarize
        """
        console.print("\n" + "=" * 60)
        console.print("[bold]Comparison Summary[/bold]")
        console.print("=" * 60)

        console.print(f"\nZip file: {result.zip_path.name}")
        console.print(f"Directory: {result.directory}")

        console.print(f"\n[bold]File counts:[/bold]")
        console.print(f"  Files in zip: {result.total_zip_files:,}")
        console.print(f"  Files in directory: {result.total_dir_files:,}")

        console.print(f"\n[bold]Comparison results:[/bold]")
        console.print(
            f"  [yellow]Duplicates (already in directory): "
            f"{len(result.duplicates):,}[/yellow] ({format_size(result.duplicate_size)})"
        )
        console.print(
            f"  [green]Unique in zip (not in directory): "
            f"{len(result.unique_in_zip):,}[/green] ({format_size(result.unique_size)})"
        )
        console.print(
            f"  [dim]Unique in directory (not in zip): "
            f"{len(result.unique_in_dir):,}[/dim]"
        )

        if result.duplicates:
            console.print(
                f"\n[yellow]You can skip extracting {len(result.duplicates)} files "
                f"({format_size(result.duplicate_size)}) - they already exist![/yellow]"
            )

        if result.unique_in_zip:
            console.print(
                f"\n[green]{len(result.unique_in_zip)} unique files "
                f"({format_size(result.unique_size)}) can be extracted.[/green]"
            )


def compare_zip_to_directory(
    zip_path: Path,
    directory: Path,
    hash_strategy: str = "size_partial"
) -> ComparisonResult:
    """
    Convenience function to compare a zip to a directory.

    Args:
        zip_path: Path to the zip file
        directory: Directory to compare against
        hash_strategy: Hashing strategy to use

    Returns:
        ComparisonResult with comparison details
    """
    comparator = ZipDirectoryComparator(hash_strategy=hash_strategy)
    return comparator.compare(zip_path, directory)
