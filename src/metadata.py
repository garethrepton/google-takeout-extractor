"""Metadata module for extracting date information from files."""
import sys
import io
import json
import zipfile
import queue
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Set, TYPE_CHECKING
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
from rich.console import Console
from scanner import ZipFileInfo

if TYPE_CHECKING:
    from cache import TakeoutCache

console = Console(legacy_windows=(sys.platform == "win32"))

# Image extensions that might have EXIF data
EXIF_EXTENSIONS = {'.jpg', '.jpeg', '.tiff', '.tif'}

# Max workers for parallel extraction
MAX_WORKERS = 16


def extract_dates_batch(
    files: List[ZipFileInfo],
    result_queue: queue.Queue,
    cache: Optional["TakeoutCache"] = None
) -> Dict[ZipFileInfo, datetime]:
    """
    Extract dates for all files efficiently with caching.

    Strategy:
    1. Check cache for previously extracted dates
    2. For non-images: use zip metadata date (instant, no I/O)
    3. For images: try EXIF first, fall back to zip date
    4. Parallelize EXIF extraction by zip file
    5. Save new dates to cache

    Args:
        files: List of ZipFileInfo objects
        result_queue: Queue for progress reporting
        cache: Optional TakeoutCache for persistence

    Returns:
        Dictionary mapping file -> datetime
    """
    file_dates: Dict[ZipFileInfo, datetime] = {}

    # Load cached dates in bulk for fast lookup
    cached_dates: Dict[tuple, datetime] = {}
    if cache:
        cached_dates = cache.get_cached_dates_bulk()
        if cached_dates:
            console.print(f"[dim]Loaded {len(cached_dates):,} cached dates[/dim]")

    # Separate files by processing needed
    needs_extraction: List[ZipFileInfo] = []
    from_cache: List[ZipFileInfo] = []

    for f in files:
        # Check cache first
        cache_key = (str(f.zip_path), f.file_path, f.file_size, f.file_crc)
        if cache_key in cached_dates:
            file_dates[f] = cached_dates[cache_key]
            from_cache.append(f)
            continue

        # All files need extraction (JSON sidecar check + optional EXIF)
        needs_extraction.append(f)

    # Report cache hits immediately
    for f in from_cache:
        result_queue.put(f)

    if from_cache:
        console.print(f"[green]✓ {len(from_cache):,} dates loaded from cache[/green]")

    # Process all files in parallel, grouped by zip (checks JSON sidecar + EXIF)
    if needs_extraction:
        console.print(f"[cyan]  Extracting dates from {len(needs_extraction):,} files...[/cyan]")

        files_by_zip: Dict[Path, List[ZipFileInfo]] = defaultdict(list)
        for f in needs_extraction:
            files_by_zip[f.zip_path].append(f)

        extraction_queue: queue.Queue = queue.Queue()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for zip_path, zip_files in files_by_zip.items():
                executor.submit(_extract_dates_for_zip, zip_path, zip_files, extraction_queue, cache)

            # Collect results
            processed = 0
            while processed < len(needs_extraction):
                try:
                    file_info, date = extraction_queue.get(timeout=0.1)
                    file_dates[file_info] = date
                    result_queue.put(file_info)
                    processed += 1
                except queue.Empty:
                    pass

    # Flush cache
    if cache:
        cache.flush()

    return file_dates


def _extract_dates_for_zip(
    zip_path: Path,
    file_infos: List[ZipFileInfo],
    result_queue: queue.Queue,
    cache: Optional["TakeoutCache"] = None
) -> None:
    """
    Extract dates for all files in a single zip archive.

    Priority: JSON sidecar > EXIF (images only) > zip metadata > zip file mtime

    Args:
        zip_path: Path to the zip archive
        file_infos: List of files to process
        result_queue: Queue to put results into
        cache: Optional cache for saving results
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Build set of JSON files for fast lookup
            json_files_in_zip: Set[str] = {
                info.filename for info in zip_ref.infolist()
                if info.filename.endswith('.json')
            }

            # Track date sources for debugging
            json_count = 0
            exif_count = 0
            zip_meta_count = 0
            fallback_count = 0

            for file_info in file_infos:
                date = None
                source = None

                # Try JSON sidecar first (most reliable for Google Takeout)
                date = _try_json_sidecar_date(zip_ref, file_info, json_files_in_zip)
                if date:
                    source = "json"
                    json_count += 1

                # Fallback to EXIF (only for images)
                if not date:
                    ext = Path(file_info.file_path).suffix.lower()
                    if ext in EXIF_EXTENSIONS and file_info.file_size > 0:
                        date = _try_exif_date(zip_ref, file_info)
                        if date:
                            source = "exif"
                            exif_count += 1

                # Fallback to zip metadata
                if not date:
                    date = file_info.get_zip_date()
                    if date:
                        source = "zip_meta"
                        zip_meta_count += 1

                # Final fallback to zip file mtime
                if not date:
                    date = datetime.fromtimestamp(zip_path.stat().st_mtime)
                    source = "fallback"
                    fallback_count += 1

                # Save to cache
                if cache:
                    cache.set_date(file_info.zip_path, file_info.file_path,
                                   file_info.file_size, file_info.file_crc, date)

                result_queue.put((file_info, date))

            # Log date source summary for this zip
            if len(file_infos) > 0:
                console.print(
                    f"[dim]  {zip_path.name}: "
                    f"json={json_count} exif={exif_count} zip={zip_meta_count} fallback={fallback_count} "
                    f"(total JSON files in zip: {len(json_files_in_zip)})[/dim]"
                )
    except Exception:
        for file_info in file_infos:
            date = file_info.get_zip_date()
            if not date:
                date = datetime.fromtimestamp(zip_path.stat().st_mtime)

            if cache:
                cache.set_date(file_info.zip_path, file_info.file_path,
                               file_info.file_size, file_info.file_crc, date)

            result_queue.put((file_info, date))


def _try_exif_date(zip_ref: zipfile.ZipFile, file_info: ZipFileInfo) -> Optional[datetime]:
    """
    Try to extract EXIF date from an image file.

    Args:
        zip_ref: Open ZipFile reference
        file_info: File to extract from

    Returns:
        datetime if found, None otherwise
    """
    try:
        with zip_ref.open(file_info.file_path) as f:
            # Read just enough to get EXIF (usually in first 64KB)
            file_bytes = io.BytesIO(f.read(65536))

            with Image.open(file_bytes) as img:
                exif_data = img._getexif()

                if not exif_data:
                    return None

                # Look for DateTimeOriginal (tag 36867) first
                if 36867 in exif_data:
                    return datetime.strptime(exif_data[36867], "%Y:%m:%d %H:%M:%S")

                # Fallback to DateTime (tag 306)
                if 306 in exif_data:
                    return datetime.strptime(exif_data[306], "%Y:%m:%d %H:%M:%S")

    except Exception:
        pass

    return None


def _try_json_sidecar_date(
    zip_ref: zipfile.ZipFile,
    file_info: ZipFileInfo,
    json_files_in_zip: Set[str]
) -> Optional[datetime]:
    """
    Try to extract date from Google Takeout JSON sidecar file.

    Google Takeout includes JSON files like 'photo.jpg.json' with metadata
    including photoTakenTime which has the actual date.

    Args:
        zip_ref: Open ZipFile reference
        file_info: File to find sidecar for
        json_files_in_zip: Set of JSON file paths in the zip for fast lookup

    Returns:
        datetime if found, None otherwise
    """
    file_path = file_info.file_path
    file_dir = str(Path(file_path).parent)
    file_name = Path(file_path).name
    file_stem = Path(file_path).stem

    # Try various sidecar naming patterns Google Takeout uses
    possible_json_paths = [
        f"{file_path}.json",                              # photo.jpg.json (most common)
        f"{file_dir}/{file_stem}.json" if file_dir != "." else f"{file_stem}.json",  # photo.json
    ]

    # Also try matching by prefix for truncated filenames
    # Google Takeout truncates long filenames but keeps the JSON
    if len(file_name) > 40:
        prefix = file_name[:40]
        for json_file in json_files_in_zip:
            json_name = Path(json_file).name
            if json_name.startswith(prefix) and json_name.endswith('.json'):
                possible_json_paths.append(json_file)

    for json_path in possible_json_paths:
        if json_path not in json_files_in_zip:
            continue

        try:
            with zip_ref.open(json_path) as f:
                data = json.load(f)

                # Only use photoTakenTime - this is the actual photo date
                # (creationTime is often the export date, not the photo date)
                if 'photoTakenTime' in data and 'timestamp' in data['photoTakenTime']:
                    timestamp = int(data['photoTakenTime']['timestamp'])
                    return datetime.fromtimestamp(timestamp)

        except Exception:
            pass

    return None


def extract_date(file_info: ZipFileInfo) -> datetime:
    """
    Extract date from a file (legacy single-file interface).

    For batch processing, use extract_dates_batch() instead.

    Args:
        file_info: ZipFileInfo object representing the file

    Returns:
        datetime object representing the file's date
    """
    zip_date = file_info.get_zip_date()

    ext = Path(file_info.file_path).suffix.lower()
    if ext in EXIF_EXTENSIONS:
        exif_date = _extract_exif_date_single(file_info)
        if exif_date:
            return exif_date

    if zip_date:
        return zip_date

    return datetime.fromtimestamp(file_info.zip_path.stat().st_mtime)


def _extract_exif_date_single(file_info: ZipFileInfo) -> Optional[datetime]:
    """Extract EXIF date from a single file (opens zip each time - slow)."""
    try:
        with zipfile.ZipFile(file_info.zip_path, 'r') as zip_ref:
            return _try_exif_date(zip_ref, file_info)
    except Exception:
        return None
