> [!WARNING]
> **This project was vibe coded!** Built entirely through AI-assisted development with [Claude Code](https://claude.ai/claude-code).
> Your mileage may vary. Use at your own risk and always backup your data first.

# Google Takeout Tool

A powerful Python CLI for analyzing, deduplicating, and extracting Google Takeout archives with intelligent organization.

![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)

## Features

- **Zero-extraction analysis** - Scan zip archives directly without extracting
- **Smart deduplication** - Detect duplicates using CRC32 checksums from zip metadata
- **EXIF date extraction** - Read photo dates for intelligent organization
- **Year/month organization** - Automatically organize files by date
- **Incremental extraction** - Process one zip at a time with cleanup prompts
- **Directory comparison** - Compare zips against existing files to skip duplicates
- **SQLite caching** - Cache EXIF dates and directory hashes for fast re-runs
- **Multi-threaded scanning** - Parallel file hashing for large directories
- **Rich TUI** - Beautiful terminal interface with progress bars and tables
- **HTML reports** - Interactive reports with charts and statistics
- **Manifest generation** - Track extraction progress across sessions

## Installation

```bash
# Clone the repository
git clone git@github.com:garethrepton/google-takeout-extractor.git
cd google-takeout-extractor

# Install dependencies
pip install -r requirements.txt
```

### Requirements

- Python 3.10+
- rich >= 13.7.0
- pillow >= 10.2.0
- python-magic-bin >= 0.4.14 (Windows)

## Commands

### 1. Analyze

Scan archives and generate an interactive HTML report.

```bash
python src/main.py analyze <input_dir> [options]
```

**Options:**
| Flag | Description |
|------|-------------|
| `-o, --output` | Output HTML filename (default: `takeout_report.html`) |
| `--no-open` | Don't auto-open the report in browser |
| `--clear-cache` | Clear cached EXIF dates and start fresh |

**Example:**
```bash
python src/main.py analyze "D:\Google Takeout" -o my_report.html
```

#### Analysis Output

```
╭──────────────────────────────────────╮
│       Google Takeout Tool            │
╰──────────────────────────────────────╯

Step 1: Scanning zip files in directory
Found 15 zip files containing 45,230 files

Step 2: Detecting duplicates
Found 12,450 duplicates (28%)

Step 3: Extracting date metadata
Extracting dates ━━━━━━━━━━━━━━━━━━━━ 100% 45,230/45,230

Step 4: Proposing extraction locations
Proposed locations for 32,780 files

Step 5: Generating report
✓ Report saved to takeout_report.html
```

#### HTML Report Preview

The HTML report includes:

```
┌─────────────────────────────────────────────────────────────┐
│  Google Takeout Analysis Report                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Total Files │  │  Unique     │  │ Duplicates  │         │
│  │   45,230    │  │   32,780    │  │   12,450    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│                                                             │
│  [PIE CHART: Files by Type]    [BAR CHART: Files by Year]  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Duplicate Groups                                     │   │
│  ├──────────────────┬──────────┬───────────────────────┤   │
│  │ Original         │ Copies   │ Space Wasted          │   │
│  ├──────────────────┼──────────┼───────────────────────┤   │
│  │ IMG_20230415.jpg │    3     │ 12.5 MB               │   │
│  │ vacation.mp4     │    2     │ 245.8 MB              │   │
│  └──────────────────┴──────────┴───────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

### 2. Extract

Extract unique files with automatic deduplication and optional cleanup.

```bash
python src/main.py extract <input_dir> [options]
```

**Options:**
| Flag | Description |
|------|-------------|
| `-d, --output-dir` | Extraction directory (default: `extracted`) |
| `-i, --incremental` | Process one zip at a time with cleanup between |
| `--dry-run` | Show what would be extracted without doing it |
| `--no-cleanup` | Skip delete prompts for source zips |
| `--auto-cleanup` | Auto-delete source zips after extraction |
| `--clear-cache` | Clear cache and start fresh |

**Example:**
```bash
# Standard extraction
python src/main.py extract "D:\Google Takeout" -d "D:\Photos"

# Incremental mode (recommended for large archives)
python src/main.py extract "D:\Google Takeout" -d "D:\Photos" -i

# Preview only
python src/main.py extract "D:\Google Takeout" --dry-run
```

#### Extraction Plan

Before extracting, you'll see a detailed plan:

```
╭─────────────────── Extraction Plan ───────────────────╮
│                                                       │
│ Analysis Complete                                     │
│                                                       │
│ Files in zips:        45,230                          │
│ To extract:           28,450 (125.6 GB)               │
│ Already in output:    4,330 (skipped)                 │
│ Duplicates:           12,450 (skipped)                │
│                                                       │
╰───────────────────────────────────────────────────────╯

        Archives Available for Cleanup
┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
┃ Archive                 ┃ Status           ┃ Files ┃ Size     ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
│ takeout-20230101.zip    │ Fully Extracted  │ 3,240 │ 4.2 GB   │
│ takeout-20230415.zip    │ 100% Duplicates  │ 1,850 │ 2.1 GB   │
├─────────────────────────┼──────────────────┼───────┼──────────┤
│ Total                   │                  │ 2 zips│ 6.3 GB   │
┗━━━━━━━━━━━━━━━━━━━━━━━━━┷━━━━━━━━━━━━━━━━━━┷━━━━━━━┷━━━━━━━━━━┛

          Files to Extract by Type
┏━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Type          ┃ Count   ┃ Size        ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━┩
│ Photos        │ 18,420  │ 45.2 GB     │
│ Videos        │ 2,340   │ 78.5 GB     │
│ Documents     │ 5,120   │ 1.2 GB      │
│ Other         │ 2,570   │ 0.7 GB      │
┗━━━━━━━━━━━━━━━┷━━━━━━━━━┷━━━━━━━━━━━━━┛

Target: D:\Photos
```

#### Extraction Progress

```
Extracting from takeout-20230415.zip
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% 3,240/3,240
2023/04/IMG_20230415_142356.jpg

✓ Extracted 3,240 files (4.2 GB)
```

#### Cleanup Prompts

After each zip (in incremental mode) or at the end:

```
╭─────────────────────────────────────────────────────────────╮
│ Cleanup: takeout-20230415.zip                               │
│                                                             │
│ Size: 4.2 GB                                                │
│ Status: All 3,240 files successfully extracted              │
│                                                             │
│ Delete this archive? [Y]es / [N]o / [A]ll / [V]iew / Ne[v]er│
╰─────────────────────────────────────────────────────────────╯
```

| Option | Action |
|--------|--------|
| `Y` | Delete this zip |
| `N` | Keep this zip |
| `A` | Delete all remaining zips |
| `V` | View files in this zip |
| `v` | Keep all remaining zips |

---

### 3. Reconcile

Compare a manifest against the output directory to track extraction progress.

```bash
python src/main.py reconcile <manifest.json> [options]
```

**Options:**
| Flag | Description |
|------|-------------|
| `-d, --output-dir` | Directory to check (default: `extracted`) |
| `--show-pending` | Show breakdown of pending files by year |

**Example:**
```bash
python src/main.py reconcile manifest.json -d "D:\Photos" --show-pending
```

#### Reconciliation Output

```
╭──────────────── Reconciliation Report ────────────────╮
│                                                       │
│ Manifest: 32,780 unique files                         │
│ Found in output: 28,450 (86.8%)                       │
│ Pending: 4,330 files                                  │
│                                                       │
╰───────────────────────────────────────────────────────╯

         Pending Files by Year
┏━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┓
┃ Year   ┃ Files  ┃ Size      ┃
┡━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━┩
│ 2023   │ 2,150  │ 8.4 GB    │
│ 2022   │ 1,480  │ 5.2 GB    │
│ 2021   │ 700    │ 2.1 GB    │
┗━━━━━━━━┷━━━━━━━━┷━━━━━━━━━━━┛
```

---

### 4. Compare

Compare a single zip against an existing directory.

```bash
python src/main.py compare <zip_file> <directory> [options]
```

**Options:**
| Flag | Description |
|------|-------------|
| `--extract` | Extract unique files after comparison |
| `-d, --output-dir` | Extraction directory (default: `extracted`) |
| `--hash-strategy` | Hash method: `size_partial`, `size_crc`, `full` |

**Hash Strategies:**
| Strategy | Speed | Accuracy | Description |
|----------|-------|----------|-------------|
| `size_partial` | Fast | Good | Size + partial file hash (64KB chunks) |
| `size_crc` | Medium | Best | Size + CRC32 (matches zip metadata) |
| `full` | Slow | Perfect | Full SHA256 hash |

**Example:**
```bash
python src/main.py compare takeout.zip "D:\Photos" --extract
```

#### Comparison Output

```
Scanning zip: takeout-20230415.zip
Found 3,240 files in zip

Scanning directory: D:\Photos
Hashing files ━━━━━━━━━━━━━━━━━━━━ 100% 45,230/45,230

╭──────────────────── Comparison Results ────────────────────╮
│                                                            │
│ Files in zip:           3,240                              │
│ Already in directory:   2,890 (89.2%)                      │
│ Unique to zip:          350                                │
│                                                            │
╰────────────────────────────────────────────────────────────╯
```

---

### 5. Diff

Compare two directories to find files in source that are missing from destination.

```bash
python src/main.py diff <source_dir> <dest_dir> [options]
```

**Options:**
| Flag | Description |
|------|-------------|
| `--hash-strategy` | Hash method: `size_partial`, `size_crc`, `full` |
| `--show-missing` | Show breakdown of missing files by folder |
| `--save-manifest FILE` | Save list of missing files to JSON |

**Example:**
```bash
# Compare two photo libraries
python src/main.py diff "D:\OldPhotos" "D:\NewPhotos" --show-missing

# Save missing files list for later
python src/main.py diff "D:\Backup" "D:\Current" --save-manifest missing.json
```

#### Diff Output

```
╭─────────────────── Directory Comparison ───────────────────╮
│                                                            │
│ Comparison Complete                                        │
│                                                            │
│ Source files:         45,230 (125.6 GB)                    │
│ In destination:       42,100 (93.1%)                       │
│ Missing:              3,130 (8.4 GB)                       │
│                                                            │
╰────────────────────────────────────────────────────────────╯

           Missing Files by Folder
┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┓
┃ Folder                  ┃ Files  ┃ Size      ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━┩
│ 2023/vacation           │ 1,240  │ 4.2 GB    │
│ 2022/christmas          │ 890    │ 2.1 GB    │
│ 2021/misc               │ 1,000  │ 2.1 GB    │
┗━━━━━━━━━━━━━━━━━━━━━━━━━┷━━━━━━━━┷━━━━━━━━━━━┛

Progress: [██████████████████████████████████████░░] 93.1%
```

---

## Output Structure

Extracted files are organized by year and month:

```
extracted/
├── 2023/
│   ├── 01/
│   │   ├── IMG_20230115_142356.jpg
│   │   └── IMG_20230118_091234.jpg
│   ├── 02/
│   │   └── video_20230214.mp4
│   └── 12/
│       └── christmas_party.jpg
├── 2022/
│   └── ...
└── extraction_report.html
```

---

## Caching

The tool caches data to speed up subsequent runs:

| Cache | Location | Contents |
|-------|----------|----------|
| EXIF dates | `~/.googletakeout/takeout_cache.db` | Extracted dates from photos |
| Directory hashes | Same database | File hashes from output directory |

Use `--clear-cache` with any command to reset the cache.

---

## Performance Tips

1. **Use incremental mode** (`-i`) for large archives - process and cleanup one at a time
2. **Run analysis first** - generate a manifest before extraction
3. **Use `size_partial` hash strategy** (default) - fastest comparison method
4. **Let the cache build** - first run is slower, subsequent runs use cached data

---

## Examples

### Full workflow

```bash
# 1. Analyze first to see what you have
python src/main.py analyze "D:\Takeout" -o report.html

# 2. Review the HTML report in your browser

# 3. Extract incrementally with cleanup prompts
python src/main.py extract "D:\Takeout" -d "D:\Photos" -i

# 4. Later, check progress with reconcile
python src/main.py reconcile manifest.json -d "D:\Photos"
```

### Quick extraction

```bash
# Extract everything, auto-delete source zips
python src/main.py extract "D:\Takeout" -d "D:\Photos" --auto-cleanup
```

### Dry run

```bash
# See what would happen without extracting
python src/main.py extract "D:\Takeout" --dry-run
```

---

## Project Structure

```
google-takeout-extractor/
├── src/
│   ├── main.py              # CLI entry point
│   ├── scanner.py           # Zip file scanning
│   ├── hasher.py            # Duplicate detection
│   ├── metadata.py          # EXIF date extraction
│   ├── organizer.py         # Date-based organization
│   ├── extractor.py         # File extraction
│   ├── cleanup.py           # Source zip cleanup
│   ├── comparator.py        # Zip vs directory comparison
│   ├── directory_scanner.py # Multi-threaded directory scanning
│   ├── cache.py             # SQLite caching
│   ├── html_exporter.py     # HTML report generation
│   ├── csv_exporter.py      # CSV export
│   └── progress_display.py  # Rich TUI components
├── .claude/
│   └── agents/              # Claude Code agent configs
├── requirements.txt
└── README.md
```

---

## Contributing

Contributions welcome! Please open an issue or PR.
