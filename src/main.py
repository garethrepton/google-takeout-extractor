"""Main entry point for Google Takeout Tool."""
import sys
import argparse
import queue
import json
from pathlib import Path
from typing import Dict, List, Tuple, Set
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table

from scanner import scan_directory, ZipFileInfo
from hasher import build_hash_map
from metadata import extract_dates_batch
from organizer import propose_location
from html_exporter import create_html_report, open_html, format_file_size, get_file_type
from progress_display import SimpleProgressDisplay
from cache import TakeoutCache
from extractor import extract_all_unique, format_size
from cleanup import CleanupManager, CleanupMode
from comparator import ZipDirectoryComparator
from directory_scanner import DirectoryScanner, DirectoryFileInfo

# Create console with legacy Windows support if needed
console = Console(legacy_windows=(sys.platform == "win32"))


def display_banner():
    """Display the application banner."""
    banner = Text("Google Takeout Tool", style="bold cyan")
    console.print(Panel(banner, expand=False))
    console.print()


def run_analysis(files: List[ZipFileInfo], cache: TakeoutCache) -> Tuple[Dict, Dict, Dict]:
    """
    Run the core analysis pipeline (steps 2-4).

    Returns:
        Tuple of (hash_map, file_dates, proposed_locations)
    """
    # Step 2: Detect duplicates using CRC32
    console.print("[bold]Step 2:[/bold] Detecting duplicates")
    hash_map = build_hash_map(files)
    console.print()

    # Step 3: Extract metadata
    console.print("[bold]Step 3:[/bold] Extracting date metadata")
    result_queue: queue.Queue = queue.Queue()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(extract_dates_batch, files, result_queue, cache)

        with SimpleProgressDisplay("Extracting dates", len(files)) as progress:
            processed = 0
            while processed < len(files):
                try:
                    file_info = result_queue.get(timeout=0.1)
                    progress.update(file_info.get_display_path())
                    processed += 1
                except:
                    pass

        file_dates = future.result()

    console.print("[green]Date extraction complete[/green]")
    console.print()

    # Step 4: Propose locations
    console.print("[bold]Step 4:[/bold] Proposing extraction locations")
    proposed_locations: Dict[ZipFileInfo, str] = {}

    for file_info in files:
        file_date = file_dates.get(file_info)
        if file_date:
            proposed_locations[file_info] = propose_location(file_info, file_date)

    console.print(f"[green]Proposed locations for {len(proposed_locations)} files[/green]")
    console.print()

    return hash_map, file_dates, proposed_locations


def generate_manifest(
    hash_map: Dict[str, List],
    file_dates: Dict,
    proposed_locations: Dict,
    output_path: Path
) -> None:
    """
    Generate a manifest file for later reconciliation.

    The manifest contains all unique files with their content keys,
    sizes, dates, and proposed destinations.
    """
    manifest = {
        "version": "1.0",
        "generated": datetime.now().isoformat(),
        "total_unique_files": len(hash_map),
        "files": []
    }

    for content_key, file_list in hash_map.items():
        first_file = file_list[0]
        file_date = file_dates.get(first_file)
        proposed = proposed_locations.get(first_file, "")

        manifest["files"].append({
            "content_key": content_key,
            "file_size": first_file.file_size,
            "file_crc": first_file.file_crc,
            "date": file_date.isoformat() if file_date else None,
            "proposed_location": proposed,
            "source_zip": str(first_file.zip_path),
            "source_path": first_file.file_path,
            "duplicate_count": len(file_list) - 1
        })

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)

    console.print(f"[green]Manifest: {len(manifest['files']):,} unique files[/green]")


def cmd_analyze(args):
    """Execute the analyze command (generate report only)."""
    cache = TakeoutCache()
    if args.clear_cache:
        cache.clear()
        console.print("[yellow]Cache cleared[/yellow]")
    else:
        cached_count = cache.get_cached_count()
        if cached_count > 0:
            console.print(f"[dim]Cache: {cached_count:,} entries (use --clear-cache to reset)[/dim]")
    console.print()

    display_banner()

    try:
        # Step 1: Scan directory
        console.print("[bold]Step 1:[/bold] Scanning zip files in directory")
        files = scan_directory(args.input_dir)

        if not files:
            console.print("[red]No files found. Exiting.[/red]")
            sys.exit(1)
        console.print()

        # Steps 2-4: Analysis
        hash_map, file_dates, proposed_locations = run_analysis(files, cache)

        # Step 5: Generate HTML report
        console.print("[bold]Step 5:[/bold] Generating HTML report")
        report_path = create_html_report(hash_map, file_dates, proposed_locations, args.output)
        console.print()

        # Step 5b: Generate manifest file for later reconciliation
        manifest_path = Path(args.output).with_suffix('.manifest.json')
        console.print("[bold]Step 5b:[/bold] Generating manifest for reconciliation")
        generate_manifest(hash_map, file_dates, proposed_locations, manifest_path)
        console.print(f"[cyan]Manifest saved to: {manifest_path}[/cyan]")
        console.print()

        # Step 6: Open report
        if not args.no_open:
            console.print("[bold]Step 6:[/bold] Opening report")
            open_html(report_path)
        else:
            console.print(f"[cyan]Report saved to: {report_path.absolute()}[/cyan]")

        console.print()
        console.print("[bold green]Analysis complete![/bold green]")
        console.print(f"[dim]Use 'reconcile {manifest_path}' to compare against output directory[/dim]")

    finally:
        cache.close()


def show_extraction_plan(
    total_files_in_zips: int,
    unique_files: Dict[ZipFileInfo, Path],
    already_extracted_count: int,
    duplicate_count: int,
    fully_extracted_zips: List[Path],
    redundant_zips: List[Path],
    zip_stats: Dict[Path, dict],
    output_dir: Path
) -> None:
    """
    Show a comprehensive extraction plan after analysis.

    Args:
        total_files_in_zips: Total files found in all zips
        unique_files: Files to be extracted with their destinations
        already_extracted_count: Files already in output directory
        duplicate_count: Duplicate files that will be skipped
        fully_extracted_zips: Zips with all files already extracted
        redundant_zips: Zips that are 100% duplicates
        zip_stats: Statistics for each zip file
        output_dir: Target extraction directory
    """
    console.print()

    # Calculate totals
    files_to_extract = len(unique_files)
    size_to_extract = sum(f.file_size for f in unique_files.keys())

    # Calculate potential space to free (zips that can be deleted)
    deletable_zips = set(fully_extracted_zips) | set(redundant_zips)
    space_to_free = 0
    for zp in deletable_zips:
        try:
            space_to_free += zp.stat().st_size
        except:
            pass

    # Main summary panel
    summary_text = Text()
    summary_text.append("Analysis Complete\n\n", style="bold white")

    summary_text.append("Files in zips:        ", style="dim")
    summary_text.append(f"{total_files_in_zips:,}\n", style="white")

    summary_text.append("To extract:           ", style="dim")
    summary_text.append(f"{files_to_extract:,}", style="bold green")
    summary_text.append(f" ({format_size(size_to_extract)})\n", style="green")

    summary_text.append("Already in output:    ", style="dim")
    summary_text.append(f"{already_extracted_count:,}", style="cyan")
    summary_text.append(" (skipped)\n", style="dim cyan")

    summary_text.append("Duplicates:           ", style="dim")
    summary_text.append(f"{duplicate_count:,}", style="yellow")
    summary_text.append(" (skipped)\n", style="dim yellow")

    console.print(Panel(summary_text, title="[bold cyan]Extraction Plan[/bold cyan]", border_style="cyan"))

    # Zip cleanup summary
    if deletable_zips:
        cleanup_table = Table(title="Archives Available for Cleanup", show_header=True, header_style="bold red")
        cleanup_table.add_column("Archive", style="dim")
        cleanup_table.add_column("Status", justify="center")
        cleanup_table.add_column("Files", justify="right")
        cleanup_table.add_column("Size", justify="right")

        for zp in sorted(deletable_zips):
            stats = zip_stats.get(zp, {})
            try:
                size = zp.stat().st_size
            except:
                size = 0

            if zp in fully_extracted_zips:
                status = "[cyan]Fully Extracted[/cyan]"
                file_count = stats.get('already_extracted', 0)
            else:
                status = "[red]100% Duplicates[/red]"
                file_count = stats.get('duplicates', 0)

            cleanup_table.add_row(zp.name, status, f"{file_count:,}", format_size(size))

        # Add total row
        cleanup_table.add_row("", "", "", "─" * 10, style="dim")
        cleanup_table.add_row("[bold]Total[/bold]", "", f"{len(deletable_zips)} zips", f"[bold]{format_size(space_to_free)}[/bold]")

        console.print(cleanup_table)
        console.print()

    # Files by type breakdown
    if unique_files:
        files_by_type: Dict[str, int] = defaultdict(int)
        size_by_type: Dict[str, int] = defaultdict(int)

        for file_info in unique_files.keys():
            file_type = get_file_type(file_info.file_path)
            files_by_type[file_type] += 1
            size_by_type[file_type] += file_info.file_size

        type_table = Table(title="Files to Extract by Type", show_header=True, header_style="bold green")
        type_table.add_column("Type", style="cyan")
        type_table.add_column("Count", justify="right")
        type_table.add_column("Size", justify="right")

        for file_type, count in sorted(files_by_type.items(), key=lambda x: x[1], reverse=True):
            type_table.add_row(file_type, f"{count:,}", format_size(size_by_type[file_type]))

        console.print(type_table)
        console.print()

    # Output location
    console.print(f"[bold]Target:[/bold] {output_dir.absolute()}")
    console.print()


def generate_extraction_report(
    output_dir: Path,
    total_extracted: int,
    total_errors: int,
    total_bytes: int,
    unique_files: Dict[ZipFileInfo, Path],
    deleted_zips: List[Path],
    total_freed: int,
    start_time: datetime,
    end_time: datetime
) -> Path:
    """
    Generate a post-extraction report.

    Args:
        output_dir: Where files were extracted
        total_extracted: Number of files successfully extracted
        total_errors: Number of extraction errors
        total_bytes: Total bytes written
        unique_files: The files that were to be extracted
        deleted_zips: List of deleted zip files
        total_freed: Space freed by deleting zips
        start_time: When extraction started
        end_time: When extraction finished

    Returns:
        Path to the generated report file
    """
    report_path = output_dir / "extraction_report.html"

    # Calculate statistics
    duration = (end_time - start_time).total_seconds()

    # Group extracted files by year
    files_by_year: Dict[int, List[Tuple[ZipFileInfo, Path]]] = defaultdict(list)
    for file_info, dest in unique_files.items():
        parts = dest.parts
        if len(parts) >= 2 and parts[1].isdigit():
            year = int(parts[1])
        else:
            year = 0
        files_by_year[year].append((file_info, dest))

    # Group by type
    files_by_type: Dict[str, int] = defaultdict(int)
    size_by_type: Dict[str, int] = defaultdict(int)
    for file_info in unique_files.keys():
        file_type = get_file_type(file_info.file_path)
        files_by_type[file_type] += 1
        size_by_type[file_type] += file_info.file_size

    # Generate year rows for table
    year_rows = ""
    for year in sorted(files_by_year.keys(), reverse=True):
        year_label = str(year) if year > 0 else "Unknown"
        file_list = files_by_year[year]
        year_size = sum(f.file_size for f, _ in file_list)
        year_rows += f"""
            <tr>
                <td>{year_label}</td>
                <td>{len(file_list):,}</td>
                <td>{format_size(year_size)}</td>
            </tr>
        """

    # Generate type rows
    type_rows = ""
    for file_type in sorted(files_by_type.keys(), key=lambda x: files_by_type[x], reverse=True):
        type_rows += f"""
            <tr>
                <td><span class="badge {file_type}">{file_type}</span></td>
                <td>{files_by_type[file_type]:,}</td>
                <td>{format_size(size_by_type[file_type])}</td>
            </tr>
        """

    # Generate deleted zips list
    deleted_rows = ""
    for zip_path in deleted_zips:
        deleted_rows += f"""
            <tr>
                <td>{zip_path.name}</td>
                <td>Deleted</td>
            </tr>
        """

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Extraction Report - {end_time.strftime("%Y-%m-%d %H:%M")}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            line-height: 1.6;
            padding: 40px;
        }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        header {{
            background: linear-gradient(135deg, #059669 0%, #0f172a 100%);
            border: 1px solid #34d399;
            padding: 30px;
            margin-bottom: 30px;
            border-radius: 16px;
        }}
        header h1 {{ font-size: 2rem; margin-bottom: 8px; color: #fff; }}
        header p {{ color: #94a3b8; }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #1e293b;
            border: 1px solid #334155;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
        }}
        .stat-card h3 {{ font-size: 2rem; color: #34d399; margin-bottom: 4px; }}
        .stat-card.warning h3 {{ color: #fbbf24; }}
        .stat-card.error h3 {{ color: #f87171; }}
        .stat-card p {{ color: #94a3b8; font-size: 0.85rem; }}
        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 1.2rem;
            color: #fff;
            margin-bottom: 16px;
            padding-bottom: 8px;
            border-bottom: 1px solid #334155;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: #1e293b;
            border-radius: 12px;
            overflow: hidden;
        }}
        th {{
            background: #0f172a;
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            color: #94a3b8;
            font-size: 0.85rem;
        }}
        td {{
            padding: 12px 16px;
            border-bottom: 1px solid #334155;
        }}
        tr:hover {{ background: #334155; }}
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .badge.image {{ background: #1e3a5f; color: #60a5fa; }}
        .badge.video {{ background: #450a0a; color: #f87171; }}
        .badge.audio {{ background: #2e1065; color: #a78bfa; }}
        .badge.document {{ background: #422006; color: #fbbf24; }}
        .badge.data {{ background: #052e16; color: #34d399; }}
        .badge.other {{ background: #1f2937; color: #9ca3af; }}
        footer {{
            text-align: center;
            padding: 30px;
            color: #64748b;
            font-size: 0.85rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>✅ Extraction Complete</h1>
            <p>Generated on {end_time.strftime("%Y-%m-%d at %H:%M:%S")} • Duration: {duration:.1f} seconds</p>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>{total_extracted:,}</h3>
                <p>Files Extracted</p>
            </div>
            <div class="stat-card">
                <h3>{format_size(total_bytes)}</h3>
                <p>Total Size</p>
            </div>
            <div class="stat-card {'error' if total_errors > 0 else ''}">
                <h3>{total_errors}</h3>
                <p>Errors</p>
            </div>
            <div class="stat-card warning">
                <h3>{len(deleted_zips)}</h3>
                <p>Zips Deleted</p>
            </div>
            <div class="stat-card">
                <h3>{format_size(total_freed)}</h3>
                <p>Space Freed</p>
            </div>
        </div>

        <div class="section">
            <h3 class="section-title">📅 Files by Year</h3>
            <table>
                <thead>
                    <tr>
                        <th>Year</th>
                        <th>Files</th>
                        <th>Size</th>
                    </tr>
                </thead>
                <tbody>
                    {year_rows}
                </tbody>
            </table>
        </div>

        <div class="section">
            <h3 class="section-title">📁 Files by Type</h3>
            <table>
                <thead>
                    <tr>
                        <th>Type</th>
                        <th>Files</th>
                        <th>Size</th>
                    </tr>
                </thead>
                <tbody>
                    {type_rows}
                </tbody>
            </table>
        </div>

        {f'''
        <div class="section">
            <h3 class="section-title">🗑️ Deleted Archives</h3>
            <table>
                <thead>
                    <tr>
                        <th>Archive</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {deleted_rows}
                </tbody>
            </table>
        </div>
        ''' if deleted_zips else ''}

        <div class="section">
            <h3 class="section-title">📂 Output Location</h3>
            <p style="background: #1e293b; padding: 16px; border-radius: 8px; font-family: monospace;">
                {output_dir.absolute()}
            </p>
        </div>

        <footer>
            <p>Google Takeout Tool • Extraction Report</p>
        </footer>
    </div>
</body>
</html>'''

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return report_path


def cmd_extract(args):
    """Execute the extract command (extract files with optional cleanup)."""
    cache = TakeoutCache()
    if args.clear_cache:
        cache.clear()
        console.print("[yellow]Cache cleared[/yellow]")
    console.print()

    display_banner()

    try:
        # Step 1: Scan directory
        console.print("[bold]Step 1:[/bold] Scanning zip files in directory")
        files = scan_directory(args.input_dir)

        if not files:
            console.print("[red]No files found. Exiting.[/red]")
            sys.exit(1)
        console.print()

        # Step 1b: Scan output directory for already-extracted files (with caching)
        output_dir = Path(args.output_dir)
        existing_hashes: Set[str] = set()
        existing_by_size: Dict[int, Set[str]] = defaultdict(set)
        if output_dir.exists():
            console.print("[bold]Step 1b:[/bold] Scanning output directory for existing files")

            # Check if we can use cached data entirely
            last_scan = cache.get_directory_last_scan(output_dir)
            use_cache_only = False

            if last_scan:
                last_scan_time, cached_file_count = last_scan
                # Check if directory has been modified since last scan
                # by finding the newest file mtime
                try:
                    newest_mtime = 0.0
                    file_count = 0
                    for f in output_dir.rglob("*"):
                        if f.is_file():
                            file_count += 1
                            mtime = f.stat().st_mtime
                            if mtime > newest_mtime:
                                newest_mtime = mtime

                    # If file count matches and no file is newer than last scan, use cache
                    if file_count == cached_file_count and newest_mtime < last_scan_time.timestamp():
                        use_cache_only = True
                        console.print(f"[dim]Using cached data ({cached_file_count:,} files, scanned {last_scan_time.strftime('%Y-%m-%d %H:%M')})[/dim]")
                except OSError:
                    pass

            if use_cache_only:
                # Load directly from cache
                existing_by_size = cache.get_directory_content_keys(output_dir)
                for keys in existing_by_size.values():
                    existing_hashes.update(keys)
            else:
                # Need to scan and verify
                cached_files = cache.get_directory_files(output_dir)
                scanner = DirectoryScanner(hash_strategy="size_partial")
                files_to_hash = []
                files_from_cache = 0
                new_cache_entries = []

                # Get all files in directory
                from rich.progress import Progress, SpinnerColumn, TextColumn

                all_dir_files = list(output_dir.rglob("*"))
                all_dir_files = [f for f in all_dir_files if f.is_file()]

                if cached_files:
                    console.print(f"[dim]Checking {len(all_dir_files):,} files against {len(cached_files):,} cached...[/dim]")

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task(f"Scanning...", total=None)

                    for file_path in all_dir_files:
                        try:
                            rel_path = str(file_path.relative_to(output_dir))
                            stat = file_path.stat()
                            file_size = stat.st_size
                            file_mtime = stat.st_mtime

                            # Check if in cache with matching mtime
                            if rel_path in cached_files:
                                cached_size, cached_mtime, cached_key = cached_files[rel_path]
                                if cached_size == file_size and abs(cached_mtime - file_mtime) < 1:
                                    # Use cached hash
                                    existing_hashes.add(cached_key)
                                    existing_by_size[file_size].add(cached_key)
                                    files_from_cache += 1
                                    continue

                            # Need to compute hash for this file
                            files_to_hash.append(file_path)
                        except (OSError, ValueError):
                            pass

                # Hash files that aren't cached
                if files_to_hash:
                    console.print(f"[dim]Hashing {len(files_to_hash):,} new/changed files...[/dim]")
                    new_files = scanner.scan_files(files_to_hash)

                    for f in new_files:
                        content_key = f.get_content_key()
                        existing_hashes.add(content_key)
                        existing_by_size[f.file_size].add(content_key)

                        # Add to cache
                        rel_path = str(f.file_path.relative_to(output_dir))
                        new_cache_entries.append((rel_path, f.file_size, f.file_path.stat().st_mtime, content_key))

                    # Save new entries to cache
                    if new_cache_entries:
                        cache.set_directory_files_bulk(output_dir, new_cache_entries)

                # Update last scan timestamp
                cache.set_directory_last_scan(output_dir, len(all_dir_files))

            if existing_hashes:
                console.print(f"[cyan]Found {len(existing_hashes):,} existing files in output directory[/cyan]")
            console.print()

        # Steps 2-4: Analysis
        hash_map, file_dates, proposed_locations = run_analysis(files, cache)

        # Step 5: Identify unique files (first occurrence of each content key)
        console.print("[bold]Step 5:[/bold] Identifying unique files for extraction")

        # Track which files are originals vs duplicates
        original_files: set = set()
        duplicate_files: set = set()
        for content_key, file_list in hash_map.items():
            original_files.add(file_list[0])
            for f in file_list[1:]:
                duplicate_files.add(f)

        # Build unique files dict (only originals with proposed locations)
        # Also filter out files that already exist in the output directory
        unique_files: Dict[ZipFileInfo, Path] = {}
        already_extracted_count = 0
        already_extracted_files: Set[ZipFileInfo] = set()  # Track which files are already extracted

        # If we have existing files, we need to compute partial hashes for comparison
        if existing_by_size:
            import hashlib
            import zipfile as zf
            from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

            # First pass: separate files that definitely need extraction (no size match)
            # from those that need hash comparison
            needs_hash_check: List[ZipFileInfo] = []
            for content_key, file_list in hash_map.items():
                first_file = file_list[0]
                if first_file not in proposed_locations:
                    continue

                # Fast path: if no files with this size exist, definitely new
                if first_file.file_size not in existing_by_size:
                    proposed = proposed_locations[first_file]
                    dest = output_dir / Path(proposed).relative_to(Path(proposed).parts[0])
                    unique_files[first_file] = dest
                else:
                    needs_hash_check.append(first_file)

            # Second pass: batch process files that need hash comparison, grouped by zip
            if needs_hash_check:
                console.print(f"[dim]Comparing {len(needs_hash_check):,} files against output directory...[/dim]")

                # Group by zip for efficient batch reads
                files_by_zip_check: Dict[Path, List[ZipFileInfo]] = defaultdict(list)
                for f in needs_hash_check:
                    files_by_zip_check[f.zip_path].append(f)

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=40),
                    TaskProgressColumn(),
                    console=console
                ) as progress:
                    task = progress.add_task("Checking files...", total=len(needs_hash_check))

                    for zip_path, zip_files in files_by_zip_check.items():
                        try:
                            with zf.ZipFile(zip_path, 'r') as zip_ref:
                                for file_info in zip_files:
                                    try:
                                        with zip_ref.open(file_info.file_path) as f:
                                            content = f.read()

                                        # Compute partial hash
                                        hasher = hashlib.sha256()
                                        sample_size = 64 * 1024
                                        file_size = len(content)

                                        hasher.update(content[:sample_size])
                                        if file_size > sample_size * 2:
                                            middle_start = file_size // 2
                                            hasher.update(content[middle_start:middle_start + sample_size])
                                        if file_size > sample_size:
                                            hasher.update(content[-sample_size:])

                                        partial_hash = f"{file_size}_{hasher.hexdigest()[:16]}"

                                        if partial_hash in existing_by_size[file_info.file_size]:
                                            already_extracted_count += 1
                                            already_extracted_files.add(file_info)
                                        else:
                                            proposed = proposed_locations[file_info]
                                            dest = output_dir / Path(proposed).relative_to(Path(proposed).parts[0])
                                            unique_files[file_info] = dest
                                    except Exception:
                                        # If we can't read, assume it needs extraction
                                        proposed = proposed_locations[file_info]
                                        dest = output_dir / Path(proposed).relative_to(Path(proposed).parts[0])
                                        unique_files[file_info] = dest

                                    progress.update(task, advance=1)
                        except Exception:
                            # If zip is bad, add all files from it
                            for file_info in zip_files:
                                proposed = proposed_locations[file_info]
                                dest = output_dir / Path(proposed).relative_to(Path(proposed).parts[0])
                                unique_files[file_info] = dest
                                progress.update(task, advance=1)
        else:
            # No existing files, just add all unique files
            for content_key, file_list in hash_map.items():
                first_file = file_list[0]
                if first_file in proposed_locations:
                    proposed = proposed_locations[first_file]
                    dest = output_dir / Path(proposed).relative_to(Path(proposed).parts[0])
                    unique_files[first_file] = dest

        # Find ALL zips and identify redundant/fully-extracted ones
        all_zips: set = {f.zip_path for f in files}
        zip_stats: Dict[Path, dict] = {}
        for zip_path in all_zips:
            zip_files = [f for f in files if f.zip_path == zip_path]
            # Count files that need extraction (originals not already extracted)
            needs_extraction = sum(1 for f in zip_files if f in original_files and f not in already_extracted_files)
            # Count files already in output
            already_in_output = sum(1 for f in zip_files if f in already_extracted_files)
            # Count duplicate files (within the zip set)
            dup_count = sum(1 for f in zip_files if f in duplicate_files)
            zip_stats[zip_path] = {
                'total': len(zip_files),
                'needs_extraction': needs_extraction,
                'already_extracted': already_in_output,
                'duplicates': dup_count,
                'is_redundant': needs_extraction == 0 and dup_count > 0 and already_in_output == 0,
                'is_fully_extracted': needs_extraction == 0 and already_in_output > 0
            }

        redundant_zips = [zp for zp, stats in zip_stats.items() if stats['is_redundant']]
        fully_extracted_zips = [zp for zp, stats in zip_stats.items() if stats['is_fully_extracted']]

        duplicate_count = len(files) - len(unique_files) - already_extracted_count
        console.print(f"[green]{len(unique_files)} unique files to extract[/green]")
        if already_extracted_count > 0:
            console.print(f"[cyan]{already_extracted_count} files already in output directory (skipped)[/cyan]")
        if duplicate_count > 0:
            console.print(f"[yellow]{duplicate_count} duplicates will be skipped[/yellow]")
        if fully_extracted_zips:
            console.print(f"[cyan]{len(fully_extracted_zips)} zip(s) fully extracted (can be deleted)[/cyan]")
        if redundant_zips:
            console.print(f"[red]{len(redundant_zips)} zip(s) are 100% duplicates (can be deleted)[/red]")
        console.print()

        # Show extraction plan summary
        show_extraction_plan(
            total_files_in_zips=len(files),
            unique_files=unique_files,
            already_extracted_count=already_extracted_count,
            duplicate_count=duplicate_count,
            fully_extracted_zips=fully_extracted_zips,
            redundant_zips=redundant_zips,
            zip_stats=zip_stats,
            output_dir=Path(args.output_dir)
        )

        if args.dry_run:
            console.print("[bold cyan]DRY RUN - No files will be extracted[/bold cyan]")
            console.print("[bold green]Dry run complete![/bold green]")
            return

        # Track extraction timing
        start_time = datetime.now()
        deleted_zips: List[Path] = []

        # Step 6: Group by zip for ordered extraction
        files_by_zip: Dict[Path, List[Tuple[ZipFileInfo, Path]]] = defaultdict(list)
        for file_info, dest in unique_files.items():
            files_by_zip[file_info.zip_path].append((file_info, dest))

        # Determine cleanup mode
        if args.auto_cleanup:
            cleanup_mode = CleanupMode.AUTO_DELETE
        elif args.no_cleanup:
            cleanup_mode = CleanupMode.SKIP
        else:
            cleanup_mode = CleanupMode.PROMPT

        cleanup_manager = CleanupManager(mode=cleanup_mode)
        total_freed = 0
        total_extracted = 0
        total_errors = 0
        total_bytes = 0

        # Step 6: Extract files (incremental or batch mode)
        if args.incremental and unique_files:
            # Incremental mode: process one zip at a time
            console.print("[bold]Step 6:[/bold] Incremental extraction (one zip at a time)")
            console.print()

            sorted_zips = sorted(files_by_zip.keys())
            for i, zip_path in enumerate(sorted_zips, 1):
                zip_files = files_by_zip[zip_path]
                file_count = len(zip_files)
                zip_size = sum(f.file_size for f, _ in zip_files)

                # Show what we're about to extract
                console.print(Panel(
                    f"[bold]{zip_path.name}[/bold]\n"
                    f"Files to extract: {file_count:,}\n"
                    f"Size: {format_size(zip_size)}",
                    title=f"[cyan]Zip {i} of {len(sorted_zips)}[/cyan]",
                    border_style="cyan"
                ))

                # Extract this zip
                extracted, errors, bytes_written = extract_all_unique({zip_path: zip_files}, console)
                total_extracted += extracted
                total_errors += errors
                total_bytes += bytes_written

                # Offer cleanup for this zip
                if not args.no_cleanup:
                    result = cleanup_manager.prompt_cleanup(zip_path, extracted, errors)
                    if result.deleted:
                        total_freed += result.size_freed
                        deleted_zips.append(zip_path)

                # Ask to continue (unless it's the last one or auto mode)
                if i < len(sorted_zips) and cleanup_mode == CleanupMode.PROMPT:
                    console.print()
                    response = console.input("[bold]Continue to next zip? [Y]es / [N]o / [A]ll remaining: [/bold]").strip().lower()
                    if response in ('n', 'no'):
                        console.print("[yellow]Stopping incremental extraction[/yellow]")
                        break
                    elif response in ('a', 'all'):
                        # Switch to batch mode for remaining
                        remaining_zips = {zp: files_by_zip[zp] for zp in sorted_zips[i:]}
                        if remaining_zips:
                            console.print(f"[cyan]Extracting remaining {len(remaining_zips)} zips...[/cyan]")
                            ext, err, byt = extract_all_unique(remaining_zips, console)
                            total_extracted += ext
                            total_errors += err
                            total_bytes += byt
                            # Cleanup remaining
                            for zp in remaining_zips.keys():
                                result = cleanup_manager.prompt_cleanup(zp, len(remaining_zips[zp]), 0)
                                if result.deleted:
                                    total_freed += result.size_freed
                                    deleted_zips.append(zp)
                        break
                console.print()

        elif unique_files:
            # Standard mode: extract and cleanup per zip (no pauses between)
            console.print("[bold]Step 6:[/bold] Extracting files")
            console.print()

            sorted_zips = sorted(files_by_zip.keys())
            for i, zip_path in enumerate(sorted_zips, 1):
                zip_files = files_by_zip[zip_path]
                file_count = len(zip_files)
                zip_size = sum(f.file_size for f, _ in zip_files)

                console.print(f"[bold cyan][{i}/{len(sorted_zips)}][/bold cyan] {zip_path.name} ({file_count:,} files, {format_size(zip_size)})")

                # Extract this zip
                extracted, errors, bytes_written = extract_all_unique({zip_path: zip_files}, console)
                total_extracted += extracted
                total_errors += errors
                total_bytes += bytes_written

                # Cleanup for this zip immediately after extraction
                if not args.no_cleanup:
                    result = cleanup_manager.prompt_cleanup(zip_path, extracted, errors)
                    if result.deleted:
                        total_freed += result.size_freed
                        deleted_zips.append(zip_path)

                console.print()
        else:
            console.print("[bold]Step 6:[/bold] No new files to extract")
            console.print("[cyan]All files already exist in output directory[/cyan]")
            console.print()

        # Cleanup for fully extracted zips (all files already in output)
        if fully_extracted_zips and not args.no_cleanup:
            console.print()
            console.print("[bold cyan]Fully extracted archives (all files already in output):[/bold cyan]")
            for zip_path in sorted(fully_extracted_zips):
                stats = zip_stats[zip_path]
                console.print(f"  {zip_path.name}: {stats['already_extracted']} files already extracted")

                result = cleanup_manager.prompt_cleanup(zip_path, stats['already_extracted'], 0)
                if result.deleted:
                    total_freed += result.size_freed
                    deleted_zips.append(zip_path)

        # Cleanup for redundant zips (100% duplicates - nothing extracted)
        if redundant_zips and not args.no_cleanup:
            console.print()
            console.print("[bold red]Redundant archives (100% duplicates):[/bold red]")
            for zip_path in sorted(redundant_zips):
                stats = zip_stats[zip_path]
                console.print(f"  {zip_path.name}: {stats['duplicates']} duplicate files (0 unique)")

                result = cleanup_manager.prompt_cleanup(zip_path, 0, 0)
                if result.deleted:
                    total_freed += result.size_freed
                    deleted_zips.append(zip_path)

            console.print()

        # Track end time and generate report
        end_time = datetime.now()

        # Generate extraction report
        console.print("[bold]Step 8:[/bold] Generating extraction report")
        report_path = generate_extraction_report(
            output_dir=Path(args.output_dir),
            total_extracted=total_extracted,
            total_errors=total_errors,
            total_bytes=total_bytes,
            unique_files=unique_files,
            deleted_zips=deleted_zips,
            total_freed=total_freed,
            start_time=start_time,
            end_time=end_time
        )
        console.print(f"[green]Report saved to: {report_path}[/green]")
        console.print()

        # Final TUI summary
        duration = (end_time - start_time).total_seconds()

        # Build summary table
        summary_table = Table(title="Extraction Summary", show_header=True, header_style="bold green")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", justify="right")

        summary_table.add_row("Files Extracted", f"{total_extracted:,}")
        summary_table.add_row("Data Written", format_size(total_bytes))
        summary_table.add_row("Already in Output", f"{already_extracted_count:,}")
        summary_table.add_row("Duplicates Skipped", f"{duplicate_count:,}")
        if total_errors > 0:
            summary_table.add_row("[red]Errors[/red]", f"[red]{total_errors}[/red]")
        summary_table.add_row("Zips Deleted", f"{len(deleted_zips)}")
        summary_table.add_row("Space Freed", format_size(total_freed))
        summary_table.add_row("Duration", f"{duration:.1f}s")

        console.print()
        console.print(Panel(summary_table, title="[bold green]✓ Extraction Complete[/bold green]", border_style="green"))

        # Show deleted zips if any
        if deleted_zips:
            deleted_table = Table(title="Deleted Archives", show_header=True, header_style="bold red")
            deleted_table.add_column("Archive", style="dim")
            deleted_table.add_column("Status", justify="center")
            for zp in deleted_zips:
                deleted_table.add_row(zp.name, "[green]✓ Deleted[/green]")
            console.print(deleted_table)

        # Show output location and report
        console.print()
        console.print(f"[bold]Output:[/bold] {Path(args.output_dir).absolute()}")
        console.print(f"[bold]Report:[/bold] {report_path.absolute()}")

    finally:
        cache.close()


def cmd_reconcile(args):
    """Execute the reconcile command (compare manifest against output directory)."""
    manifest_path = Path(args.manifest)
    output_dir = Path(args.output_dir)

    if not manifest_path.exists():
        console.print(f"[red]Error: Manifest file not found: {manifest_path}[/red]")
        sys.exit(1)

    display_banner()

    # Load manifest
    console.print("[bold]Step 1:[/bold] Loading manifest")
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    total_files = len(manifest['files'])
    console.print(f"[cyan]Manifest: {total_files:,} unique files (generated {manifest['generated']})[/cyan]")
    console.print()

    # Scan output directory
    console.print("[bold]Step 2:[/bold] Scanning output directory")
    cache = TakeoutCache()

    try:
        if not output_dir.exists():
            console.print(f"[yellow]Output directory does not exist: {output_dir}[/yellow]")
            existing_by_size: Dict[int, Set[str]] = {}
        else:
            # Use the same caching logic as extract
            scanner = DirectoryScanner(hash_strategy="size_partial")
            existing_files = scanner.scan_directory(output_dir)
            existing_by_size = defaultdict(set)
            for f in existing_files:
                existing_by_size[f.file_size].add(f.get_content_key())

        console.print()

        # Compare manifest against output
        console.print("[bold]Step 3:[/bold] Reconciling")

        extracted = []
        pending = []
        missing_source = []

        for entry in manifest['files']:
            file_size = entry['file_size']
            content_key = entry['content_key']

            # Check if file exists in output (by size + partial hash)
            # We need to match the content_key format
            if file_size in existing_by_size:
                # Check if any existing file matches
                found = False
                for existing_key in existing_by_size[file_size]:
                    # Content keys are in format "size_hash" for partial or "size_crc" for zip
                    # We need to check if the file content matches
                    if existing_key == content_key:
                        found = True
                        break
                if found:
                    extracted.append(entry)
                    continue

            # Check if source zip still exists
            source_zip = Path(entry['source_zip'])
            if not source_zip.exists():
                missing_source.append(entry)
            else:
                pending.append(entry)

        # Display results
        console.print()

        # Summary table
        summary_table = Table(title="Reconciliation Summary", show_header=True, header_style="bold cyan")
        summary_table.add_column("Status", style="dim")
        summary_table.add_column("Count", justify="right")
        summary_table.add_column("Percentage", justify="right")

        pct_extracted = (len(extracted) / total_files * 100) if total_files > 0 else 0
        pct_pending = (len(pending) / total_files * 100) if total_files > 0 else 0
        pct_missing = (len(missing_source) / total_files * 100) if total_files > 0 else 0

        summary_table.add_row("[green]Extracted[/green]", f"{len(extracted):,}", f"{pct_extracted:.1f}%")
        summary_table.add_row("[yellow]Pending[/yellow]", f"{len(pending):,}", f"{pct_pending:.1f}%")
        if missing_source:
            summary_table.add_row("[red]Source Missing[/red]", f"{len(missing_source):,}", f"{pct_missing:.1f}%")
        summary_table.add_row("", "", "")
        summary_table.add_row("[bold]Total[/bold]", f"{total_files:,}", "100%")

        console.print(summary_table)
        console.print()

        # Show pending files by year if requested
        if pending and args.show_pending:
            pending_by_year: Dict[str, int] = defaultdict(int)
            for entry in pending:
                if entry['date']:
                    year = entry['date'][:4]
                else:
                    year = "Unknown"
                pending_by_year[year] += 1

            pending_table = Table(title="Pending Files by Year", show_header=True, header_style="bold yellow")
            pending_table.add_column("Year", style="cyan")
            pending_table.add_column("Count", justify="right")

            for year in sorted(pending_by_year.keys(), reverse=True):
                pending_table.add_row(year, f"{pending_by_year[year]:,}")

            console.print(pending_table)
            console.print()

        # Show missing source zips
        if missing_source:
            missing_zips = set(entry['source_zip'] for entry in missing_source)
            console.print("[bold red]Missing source zips:[/bold red]")
            for zip_path in sorted(missing_zips):
                count = sum(1 for e in missing_source if e['source_zip'] == zip_path)
                console.print(f"  {Path(zip_path).name}: {count:,} files")
            console.print()

        # Progress bar visualization
        if total_files > 0:
            bar_width = 50
            extracted_width = int(bar_width * len(extracted) / total_files)
            pending_width = int(bar_width * len(pending) / total_files)
            missing_width = bar_width - extracted_width - pending_width

            bar = "[green]" + "█" * extracted_width + "[/green]"
            bar += "[yellow]" + "█" * pending_width + "[/yellow]"
            bar += "[red]" + "█" * missing_width + "[/red]"

            console.print(f"Progress: [{bar}] {pct_extracted:.1f}%")
            console.print()

        console.print("[bold green]Reconciliation complete![/bold green]")

    finally:
        cache.close()


def cmd_compare(args):
    """Execute the compare command (compare zip to directory)."""
    zip_path = Path(args.zip_file)
    directory = Path(args.directory)

    if not zip_path.exists():
        console.print(f"[red]Error: Zip file not found: {zip_path}[/red]")
        sys.exit(1)

    if not directory.exists():
        console.print(f"[red]Error: Directory not found: {directory}[/red]")
        sys.exit(1)

    display_banner()

    # Run comparison
    comparator = ZipDirectoryComparator(hash_strategy=args.hash_strategy)
    result = comparator.compare(zip_path, directory)

    # Print summary
    comparator.print_summary(result)

    # Optional extraction
    if args.extract and result.unique_in_zip:
        console.print(f"\n[bold]Extracting {len(result.unique_in_zip)} unique files...[/bold]")

        # Get dates for unique files
        cache = TakeoutCache()
        try:
            result_queue: queue.Queue = queue.Queue()

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    extract_dates_batch, result.unique_in_zip, result_queue, cache
                )

                with SimpleProgressDisplay("Extracting dates", len(result.unique_in_zip)) as progress:
                    processed = 0
                    while processed < len(result.unique_in_zip):
                        try:
                            file_info = result_queue.get(timeout=0.1)
                            progress.update(file_info.get_display_path())
                            processed += 1
                        except:
                            pass

                file_dates = future.result()

            # Build extraction list
            files_by_zip: Dict[Path, List[Tuple[ZipFileInfo, Path]]] = defaultdict(list)
            for file_info in result.unique_in_zip:
                file_date = file_dates.get(file_info, datetime.now())
                proposed = propose_location(file_info, file_date)
                dest = Path(args.output_dir) / Path(proposed).relative_to(Path(proposed).parts[0])
                files_by_zip[file_info.zip_path].append((file_info, dest))

            # Extract
            total_extracted, total_errors, total_bytes = extract_all_unique(files_by_zip, console)

            console.print()
            console.print("[bold green]Extraction complete![/bold green]")
            console.print(f"  Files extracted: {total_extracted:,}")
            console.print(f"  Bytes written: {format_size(total_bytes)}")

        finally:
            cache.close()


def cmd_diff(args):
    """Execute the diff command (compare two directories)."""
    source_dir = Path(args.source_dir)
    dest_dir = Path(args.dest_dir)

    if not source_dir.exists():
        console.print(f"[red]Error: Source directory not found: {source_dir}[/red]")
        sys.exit(1)

    if not dest_dir.exists():
        console.print(f"[red]Error: Destination directory not found: {dest_dir}[/red]")
        sys.exit(1)

    display_banner()

    cache = TakeoutCache()
    try:
        # Step 1: Scan source directory
        console.print("[bold]Step 1:[/bold] Scanning source directory")
        source_scanner = DirectoryScanner(hash_strategy=args.hash_strategy)
        source_files = source_scanner.scan_directory(source_dir)
        console.print(f"[cyan]Found {len(source_files):,} files in source[/cyan]")
        console.print()

        # Build source index by content key
        source_by_key: Dict[str, List[DirectoryFileInfo]] = defaultdict(list)
        source_total_size = 0
        for f in source_files:
            source_by_key[f.get_content_key()].append(f)
            source_total_size += f.file_size

        # Step 2: Scan destination directory
        console.print("[bold]Step 2:[/bold] Scanning destination directory")
        dest_scanner = DirectoryScanner(hash_strategy=args.hash_strategy)
        dest_files = dest_scanner.scan_directory(dest_dir)
        console.print(f"[cyan]Found {len(dest_files):,} files in destination[/cyan]")
        console.print()

        # Build destination index
        dest_keys = set()
        for f in dest_files:
            dest_keys.add(f.get_content_key())

        # Step 3: Compare
        console.print("[bold]Step 3:[/bold] Comparing directories")

        # Find files in source but not in destination
        missing_files = []
        present_files = []

        for content_key, files in source_by_key.items():
            if content_key in dest_keys:
                present_files.extend(files)
            else:
                missing_files.extend(files)

        missing_size = sum(f.file_size for f in missing_files)
        present_size = sum(f.file_size for f in present_files)

        console.print()

        # Summary panel
        summary_text = Text()
        summary_text.append("Comparison Complete\n\n", style="bold white")

        summary_text.append("Source files:         ", style="dim")
        summary_text.append(f"{len(source_files):,}", style="white")
        summary_text.append(f" ({format_size(source_total_size)})\n", style="dim")

        summary_text.append("In destination:       ", style="dim")
        summary_text.append(f"{len(present_files):,}", style="bold green")
        pct_present = (len(present_files) / len(source_files) * 100) if source_files else 0
        summary_text.append(f" ({pct_present:.1f}%)\n", style="green")

        summary_text.append("Missing:              ", style="dim")
        summary_text.append(f"{len(missing_files):,}", style="bold yellow")
        summary_text.append(f" ({format_size(missing_size)})\n", style="yellow")

        console.print(Panel(summary_text, title="[bold cyan]Directory Comparison[/bold cyan]", border_style="cyan"))

        # Show missing files breakdown by folder if requested
        if args.show_missing and missing_files:
            # Group by parent folder
            by_folder: Dict[str, List[DirectoryFileInfo]] = defaultdict(list)
            for f in missing_files:
                rel_path = f.file_path.relative_to(source_dir)
                folder = str(rel_path.parent) if rel_path.parent != Path('.') else "(root)"
                by_folder[folder].append(f)

            folder_table = Table(title="Missing Files by Folder", show_header=True, header_style="bold yellow")
            folder_table.add_column("Folder", style="dim")
            folder_table.add_column("Files", justify="right")
            folder_table.add_column("Size", justify="right")

            for folder in sorted(by_folder.keys())[:20]:  # Limit to top 20 folders
                files = by_folder[folder]
                folder_size = sum(f.file_size for f in files)
                folder_table.add_row(folder, f"{len(files):,}", format_size(folder_size))

            if len(by_folder) > 20:
                folder_table.add_row(f"... and {len(by_folder) - 20} more folders", "", "")

            console.print(folder_table)
            console.print()

        # Progress bar
        if source_files:
            bar_width = 40
            present_width = int(pct_present / 100 * bar_width)
            missing_width = bar_width - present_width

            bar = "[green]" + "█" * present_width + "[/green]"
            bar += "[yellow]" + "█" * missing_width + "[/yellow]"

            console.print(f"Progress: [{bar}] {pct_present:.1f}%")
            console.print()

        # Optionally save manifest of missing files
        if args.save_manifest:
            manifest = {
                "generated": datetime.now().isoformat(),
                "source_dir": str(source_dir.absolute()),
                "dest_dir": str(dest_dir.absolute()),
                "total_source_files": len(source_files),
                "missing_count": len(missing_files),
                "files": [
                    {
                        "path": str(f.file_path.relative_to(source_dir)),
                        "file_size": f.file_size,
                        "content_key": f.get_content_key()
                    }
                    for f in missing_files
                ]
            }

            manifest_path = Path(args.save_manifest)
            with open(manifest_path, 'w', encoding='utf-8') as mf:
                json.dump(manifest, mf, indent=2)

            console.print(f"[cyan]Manifest saved to: {manifest_path}[/cyan]")

        console.print("[bold green]Comparison complete![/bold green]")

    finally:
        cache.close()


def main():
    """Main function to handle command routing."""
    parser = argparse.ArgumentParser(
        description="Google Takeout Tool - Analyze, extract, and organize Google Takeout archives"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # === ANALYZE subcommand (default) ===
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze takeout archives and generate HTML report"
    )
    analyze_parser.add_argument(
        "input_dir",
        help="Directory containing Google Takeout zip files"
    )
    analyze_parser.add_argument(
        "-o", "--output",
        default="takeout_report.html",
        help="Output HTML filename (default: takeout_report.html)"
    )
    analyze_parser.add_argument(
        "--no-open",
        action="store_true",
        help="Don't automatically open the report"
    )
    analyze_parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the cache and start fresh"
    )

    # === EXTRACT subcommand ===
    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract unique files from takeout archives"
    )
    extract_parser.add_argument(
        "input_dir",
        help="Directory containing Google Takeout zip files"
    )
    extract_parser.add_argument(
        "-d", "--output-dir",
        default="extracted",
        help="Base extraction directory (default: extracted)"
    )
    extract_parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip delete prompts for source zips"
    )
    extract_parser.add_argument(
        "--auto-cleanup",
        action="store_true",
        help="Auto-delete all source zips after extraction"
    )
    extract_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be extracted without doing it"
    )
    extract_parser.add_argument(
        "--incremental", "-i",
        action="store_true",
        help="Process one zip at a time with cleanup between each"
    )
    extract_parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the cache and start fresh"
    )

    # === RECONCILE subcommand ===
    reconcile_parser = subparsers.add_parser(
        "reconcile",
        help="Compare manifest against output directory to check extraction progress"
    )
    reconcile_parser.add_argument(
        "manifest",
        help="Path to the manifest.json file from analysis"
    )
    reconcile_parser.add_argument(
        "-d", "--output-dir",
        default="extracted",
        help="Output directory to check against (default: extracted)"
    )
    reconcile_parser.add_argument(
        "--show-pending",
        action="store_true",
        help="Show breakdown of pending files by year"
    )

    # === COMPARE subcommand ===
    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare zip file against existing directory"
    )
    compare_parser.add_argument(
        "zip_file",
        help="Zip file to compare"
    )
    compare_parser.add_argument(
        "directory",
        help="Directory to compare against"
    )
    compare_parser.add_argument(
        "--extract",
        action="store_true",
        help="Extract unique files after comparison"
    )
    compare_parser.add_argument(
        "-d", "--output-dir",
        default="extracted",
        help="Base extraction directory (default: extracted)"
    )
    compare_parser.add_argument(
        "--hash-strategy",
        choices=["size_partial", "size_crc", "full"],
        default="size_partial",
        help="Hash strategy: size_partial (fast), size_crc (matches zip), full (slowest)"
    )

    # === DIFF subcommand ===
    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare two directories and find missing files"
    )
    diff_parser.add_argument(
        "source_dir",
        help="Source directory (what you want to check)"
    )
    diff_parser.add_argument(
        "dest_dir",
        help="Destination directory (where files should be)"
    )
    diff_parser.add_argument(
        "--hash-strategy",
        choices=["size_partial", "size_crc", "full"],
        default="size_partial",
        help="Hash strategy: size_partial (fast), size_crc (matches zip), full (slowest)"
    )
    diff_parser.add_argument(
        "--show-missing",
        action="store_true",
        help="Show breakdown of missing files by folder"
    )
    diff_parser.add_argument(
        "--save-manifest",
        metavar="FILE",
        help="Save manifest of missing files to JSON"
    )

    # Parse arguments
    args = parser.parse_args()

    # Handle backward compatibility: no subcommand = analyze with input_dir as first positional
    if args.command is None:
        # Check if there's a positional argument that looks like a directory
        if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
            # Re-parse with 'analyze' prepended
            new_argv = ['analyze'] + sys.argv[1:]
            args = parser.parse_args(new_argv)
        else:
            parser.print_help()
            sys.exit(1)

    # Route to appropriate command handler
    if args.command == "analyze":
        cmd_analyze(args)
    elif args.command == "extract":
        cmd_extract(args)
    elif args.command == "reconcile":
        cmd_reconcile(args)
    elif args.command == "compare":
        cmd_compare(args)
    elif args.command == "diff":
        cmd_diff(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        console.print("[dim]Progress saved to cache - run again to resume[/dim]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)
