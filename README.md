# Google Takeout Tool

A Python tool for analyzing, organizing, and deduplicating Google Takeout archives.

## Overview

This tool helps you manage Google Takeout downloads by:
- Scanning through takeout **zip files** directly (no extraction needed!)
- Detecting duplicate files using SHA256 hashing
- Extracting date metadata from files (EXIF data, zip timestamps)
- Proposing organized locations based on dates (year/month structure)
- Generating a comprehensive CSV report

## Features

- **Works with Zip Files**: Scans files inside zip archives without extracting them - saves disk space!
- **Duplicate Detection**: Uses SHA256 hashing to accurately identify duplicate files
- **Smart Date Extraction**: Extracts dates from EXIF data (photos), with fallback to zip entry timestamps
- **Date-Based Organization**: Proposes locations organized by year and month
- **Beautiful TUI**: Rich progress bars and formatted output
- **CSV Export**: Generates detailed reports that can be opened in Excel or any spreadsheet application

## Installation

1. Ensure you have Python 3.10+ installed
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Basic usage:

```bash
python src/main.py <path_to_directory_with_zip_files>
```

With options:

```bash
python src/main.py <path_to_directory_with_zip_files> -o custom_output.csv --no-open
```

### Arguments

- `input_dir` (required): Directory containing Google Takeout zip files
- `-o, --output`: Output CSV filename (default: `takeout_analysis.csv`)
- `--no-open`: Don't automatically open the CSV file after generation

## CSV Output Format

The generated CSV includes the following columns:

| Column | Description |
|--------|-------------|
| Zip File | Name of the zip file containing this file |
| File Path in Zip | Path to the file inside the zip archive |
| File Hash | SHA256 hash of the file content |
| Is Duplicate | "Yes" if this file is a duplicate, "No" otherwise |
| Duplicate Of | Location of the original file (if this is a duplicate) |
| Extracted Date | Date extracted from EXIF metadata or zip timestamps |
| Proposed Location | Suggested organized location (year/month/filename) |

## Example

```bash
python src/main.py "C:\Downloads\GoogleTakeout"
```

This will:
1. Find all .zip files in the specified directory
2. Scan files inside each zip archive
3. Compute hashes and identify duplicates
4. Extract date metadata from EXIF or zip entries
5. Propose organized locations
6. Generate `takeout_analysis.csv`
7. Automatically open the CSV file

## Project Structure

```
GoogleTakeoutTool/
├── PROJECT_CONTEXT.md    # Project planning document
├── README.md             # This file
├── requirements.txt      # Python dependencies
└── src/
    ├── __init__.py
    ├── main.py           # Entry point and orchestration
    ├── scanner.py        # Zip file scanning
    ├── hasher.py         # File hashing and duplicate detection
    ├── metadata.py       # Date extraction from files in zip
    ├── organizer.py      # Location proposal logic
    └── csv_exporter.py   # CSV generation and opening
```

## Future Enhancements

See [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) for planned features including:
- File extraction to proposed locations
- Metadata fixing
- Automatic download integration
- Interactive TUI for configuration

## Requirements

- Python 3.10+
- rich >= 13.7.0
- pillow >= 10.2.0
- python-magic-bin >= 0.4.14 (for Windows)

## License

This is a personal tool created for managing Google Takeout archives.
