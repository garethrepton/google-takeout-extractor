"""Hasher module for detecting duplicates using CRC32 from zip metadata."""
import sys
import re
from pathlib import Path
from typing import Dict, List, Tuple
from collections import defaultdict
from rich.console import Console
from scanner import ZipFileInfo

console = Console(legacy_windows=(sys.platform == "win32"))

# Pattern to match Google Takeout duplicate suffixes like (1), (2), etc.
DUPLICATE_SUFFIX_PATTERN = re.compile(r'\(\d+\)(?=\.[^.]+$|$)')


def normalize_filename(filepath: str) -> str:
    """
    Normalize a filename by removing Google Takeout duplicate suffixes.

    Examples:
        IMG_1234(1).jpg -> IMG_1234.jpg
        photo(2).png -> photo.png
        video(1)(2).mp4 -> video.mp4
        document.pdf -> document.pdf

    Args:
        filepath: Full path or filename

    Returns:
        Normalized filename (just the name, not the full path)
    """
    filename = Path(filepath).name
    # Remove all (N) suffixes
    normalized = DUPLICATE_SUFFIX_PATTERN.sub('', filename)
    return normalized.lower()  # Case-insensitive comparison


def get_size_name_key(file_info: ZipFileInfo) -> Tuple[int, str]:
    """Get a (size, normalized_name) tuple for grouping potential duplicates."""
    return (file_info.file_size, normalize_filename(file_info.file_path))


def build_hash_map(files: List[ZipFileInfo]) -> Dict[str, List[ZipFileInfo]]:
    """
    Build a hash map of all files and identify duplicates.

    Uses CRC32 from zip metadata for fast duplicate detection (no file reading needed):
    1. Groups files by (size, normalized_filename)
    2. Files with unique size+name combo are skipped (can't be duplicates)
    3. For potential duplicates, uses (size, CRC32) as content fingerprint
    4. CRC32 + size provides ~64 bits of collision resistance - sufficient for duplicate detection

    Args:
        files: List of ZipFileInfo objects

    Returns:
        Dictionary mapping content_key -> list of ZipFileInfo objects with that content
    """
    hash_map: Dict[str, List[ZipFileInfo]] = {}

    # Step 1: Group files by (size, normalized_name) to find potential duplicates
    console.print("[dim]Analyzing files for potential duplicates...[/dim]")
    potential_groups: Dict[Tuple[int, str], List[ZipFileInfo]] = defaultdict(list)
    for f in files:
        key = get_size_name_key(f)
        potential_groups[key].append(f)

    # Step 2: Separate unique files from potential duplicates
    unique_files: List[ZipFileInfo] = []
    potential_duplicates: List[ZipFileInfo] = []

    for key, group in potential_groups.items():
        if len(group) == 1:
            # Only one file with this size+name - definitely unique
            unique_files.append(group[0])
        else:
            # Multiple files with same size+name - check CRC32
            potential_duplicates.extend(group)

    console.print(f"[green]✓ {len(unique_files):,} files with unique size+name (definitely unique)[/green]")
    console.print(f"[cyan]  Checking {len(potential_duplicates):,} potential duplicates using CRC32...[/cyan]")

    # Add unique files to hash_map with synthetic unique keys
    for i, f in enumerate(unique_files):
        synthetic_hash = f"__unique_{i}_{f.file_size}"
        hash_map[synthetic_hash] = [f]

    # Step 3: Use CRC32 from zip metadata for potential duplicates (instant - no I/O!)
    for f in potential_duplicates:
        # Use size + CRC32 as content fingerprint
        content_key = f.get_content_key()
        hash_map.setdefault(content_key, []).append(f)

    # Final summary
    duplicate_count = sum(len(paths) - 1 for paths in hash_map.values() if len(paths) > 1)
    unique_count = len(hash_map)

    console.print(f"[green]✓ Analysis complete (using CRC32 - no file reading needed!)[/green]")
    console.print(f"  [cyan]Unique files:[/cyan] {unique_count:,}")
    console.print(f"  [yellow]Duplicates:[/yellow] {duplicate_count:,}")

    return hash_map


def get_duplicates(hash_map: Dict[str, List[ZipFileInfo]]) -> Dict[str, List[ZipFileInfo]]:
    """
    Extract only the duplicate entries from the hash map.

    Args:
        hash_map: Hash map from build_hash_map()

    Returns:
        Dictionary containing only hashes with multiple files
    """
    return {h: paths for h, paths in hash_map.items() if len(paths) > 1}
