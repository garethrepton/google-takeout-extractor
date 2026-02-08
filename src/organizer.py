"""Organizer module for proposing file extraction locations."""
from datetime import datetime
from pathlib import Path
from scanner import ZipFileInfo


def propose_location(file_info: ZipFileInfo, file_date: datetime, base_dir: str = "extracted") -> str:
    """
    Propose an extraction location based on the file date.

    The proposed location follows the pattern: {base_dir}/{year}/{month}/{filename}
    Example: organized/2024/01/photo.jpg

    Args:
        file_info: ZipFileInfo object representing the file
        file_date: Datetime object representing the file's date
        base_dir: Base directory for organized files (default: "extracted")

    Returns:
        Proposed relative path as a string
    """
    year = file_date.strftime("%Y")
    month = file_date.strftime("%m")
    # Extract filename from the path inside the zip
    filename = Path(file_info.file_path).name

    # Construct the proposed path
    proposed_path = Path(base_dir) / year / month / filename

    return str(proposed_path)
