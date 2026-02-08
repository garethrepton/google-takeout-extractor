"""Directory scanner module for hashing files in a directory structure."""
import sys
import zlib
import hashlib
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console(legacy_windows=(sys.platform == "win32"))


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


@dataclass(frozen=True)
class DirectoryFileInfo:
    """Represents a file in a directory (comparable to ZipFileInfo)."""
    file_path: Path
    file_size: int
    file_hash: str  # Content-based hash for comparison
    mtime: float    # Modification time

    def get_content_key(self) -> str:
        """Get content key for comparison with ZipFileInfo."""
        return self.file_hash

    def get_display_path(self) -> str:
        """Get a human-readable path for display."""
        return str(self.file_path)


class DirectoryScanner:
    """Scans directories and computes hashes for duplicate detection."""

    # Common binary extensions where partial hash is reliable
    BINARY_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp',
        '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.m4v',
        '.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma',
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.rar', '.7z', '.tar', '.gz'
    }

    def __init__(self, hash_strategy: str = "size_partial"):
        """
        Initialize the scanner.

        Args:
            hash_strategy: Hashing strategy to use
                - "size_partial": Size + hash of first/middle/last 64KB (fast, reliable)
                - "size_crc": Size + CRC32 of full file (matches zip behavior, slower)
                - "full": Full SHA256 (slowest, most accurate)
        """
        self.hash_strategy = hash_strategy

    def scan_directory(
        self,
        directory: Path,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> List[DirectoryFileInfo]:
        """
        Scan a directory and compute hashes for all files.

        Args:
            directory: Directory to scan
            progress_callback: Optional callback(file_path, current, total)

        Returns:
            List of DirectoryFileInfo objects
        """
        directory = Path(directory)
        if not directory.exists():
            console.print(f"[red]Error: Directory '{directory}' does not exist[/red]")
            return []

        if not directory.is_dir():
            console.print(f"[red]Error: '{directory}' is not a directory[/red]")
            return []

        # Collect all file paths
        all_paths = list(directory.rglob("*"))
        file_paths = [p for p in all_paths if p.is_file()]

        console.print(f"[cyan]Found {len(file_paths)} files to scan[/cyan]")

        files: List[DirectoryFileInfo] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Scanning directory...", total=len(file_paths))

            for i, file_path in enumerate(file_paths):
                try:
                    stat = file_path.stat()
                    file_hash = self._compute_hash(file_path, stat.st_size)

                    files.append(DirectoryFileInfo(
                        file_path=file_path,
                        file_size=stat.st_size,
                        file_hash=file_hash,
                        mtime=stat.st_mtime
                    ))

                    if progress_callback:
                        progress_callback(str(file_path), i + 1, len(file_paths))

                except PermissionError:
                    console.print(f"[yellow]Warning: Cannot read {file_path}[/yellow]")
                except OSError as e:
                    console.print(f"[yellow]Warning: Error reading {file_path}: {e}[/yellow]")

                progress.update(
                    task,
                    advance=1,
                    description=f"Scanning directory... ({len(files)} files)"
                )

        console.print(f"[green]Scanned {len(files)} files[/green]")
        return files

    def scan_files(
        self,
        file_paths: List[Path],
        show_progress: bool = True,
        max_workers: int = 8
    ) -> List[DirectoryFileInfo]:
        """
        Scan specific files and compute hashes (multi-threaded).

        Args:
            file_paths: List of file paths to scan
            show_progress: Whether to show progress bar
            max_workers: Number of threads for parallel hashing

        Returns:
            List of DirectoryFileInfo objects
        """
        files: List[DirectoryFileInfo] = []

        def hash_file(file_path: Path) -> Optional[DirectoryFileInfo]:
            try:
                stat = file_path.stat()
                file_hash = self._compute_hash(file_path, stat.st_size)
                return DirectoryFileInfo(
                    file_path=file_path,
                    file_size=stat.st_size,
                    file_hash=file_hash,
                    mtime=stat.st_mtime
                )
            except (PermissionError, OSError):
                return None

        if show_progress:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=40),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Hashing files...", total=len(file_paths))

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(hash_file, fp): fp for fp in file_paths}

                    for future in as_completed(futures):
                        result = future.result()
                        if result:
                            files.append(result)
                        progress.update(task, advance=1)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = executor.map(hash_file, file_paths)
                files = [r for r in results if r is not None]

        return files

    def build_hash_map(
        self,
        files: List[DirectoryFileInfo]
    ) -> Dict[str, List[DirectoryFileInfo]]:
        """
        Build a hash map from file list for duplicate detection.

        Args:
            files: List of DirectoryFileInfo objects

        Returns:
            Dict mapping content_key to list of files
        """
        hash_map: Dict[str, List[DirectoryFileInfo]] = defaultdict(list)
        for f in files:
            hash_map[f.get_content_key()].append(f)
        return dict(hash_map)

    def _compute_hash(self, file_path: Path, file_size: int) -> str:
        """
        Compute hash based on the selected strategy.

        Args:
            file_path: Path to the file
            file_size: Size of the file in bytes

        Returns:
            Content key string for comparison
        """
        if self.hash_strategy == "size_partial":
            return self._compute_partial_hash(file_path, file_size)
        elif self.hash_strategy == "size_crc":
            return self._compute_crc32(file_path, file_size)
        else:  # full
            return self._compute_sha256(file_path, file_size)

    def _compute_partial_hash(
        self,
        file_path: Path,
        file_size: int,
        sample_size: int = 64 * 1024
    ) -> str:
        """
        Compute hash from file start + middle + end for speed.

        This strategy reads up to 192KB per file (3 x 64KB samples)
        which is fast while still catching most differences.

        Args:
            file_path: Path to the file
            file_size: Size of the file
            sample_size: Size of each sample chunk (default 64KB)

        Returns:
            Content key: "{file_size}_{partial_hash}"
        """
        hasher = hashlib.sha256()

        with open(file_path, 'rb') as f:
            # First chunk
            hasher.update(f.read(sample_size))

            if file_size > sample_size * 2:
                # Middle chunk
                f.seek(file_size // 2)
                hasher.update(f.read(sample_size))

            if file_size > sample_size:
                # Last chunk
                f.seek(max(0, file_size - sample_size))
                hasher.update(f.read(sample_size))

        # Use first 16 hex chars of hash (64 bits) for reasonable uniqueness
        return f"{file_size}_{hasher.hexdigest()[:16]}"

    def _compute_crc32(self, file_path: Path, file_size: int) -> str:
        """
        Compute CRC32 of entire file (matches zip behavior).

        Args:
            file_path: Path to the file
            file_size: Size of the file

        Returns:
            Content key: "{file_size}_{crc32:08x}"
        """
        crc = 0
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                crc = zlib.crc32(chunk, crc)

        return f"{file_size}_{crc & 0xffffffff:08x}"

    def _compute_sha256(self, file_path: Path, file_size: int) -> str:
        """
        Compute full SHA256 hash of file.

        Args:
            file_path: Path to the file
            file_size: Size of the file

        Returns:
            Content key: "{file_size}_{sha256}"
        """
        hasher = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                hasher.update(chunk)

        return f"{file_size}_{hasher.hexdigest()}"


def scan_and_build_hashmap(
    directory: Path,
    hash_strategy: str = "size_partial"
) -> Dict[str, List[DirectoryFileInfo]]:
    """
    Convenience function to scan a directory and build a hash map.

    Args:
        directory: Directory to scan
        hash_strategy: Hashing strategy to use

    Returns:
        Dict mapping content_key to list of files
    """
    scanner = DirectoryScanner(hash_strategy=hash_strategy)
    files = scanner.scan_directory(directory)
    return scanner.build_hash_map(files)
