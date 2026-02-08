"""HTML Exporter module for generating styled analysis reports with charts."""
import sys
import json
import webbrowser
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from collections import defaultdict
from rich.console import Console
from scanner import ZipFileInfo

console = Console(legacy_windows=(sys.platform == "win32"))


def get_file_type(file_path: str) -> str:
    """Determine file type category from extension."""
    ext = Path(file_path).suffix.lower()

    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic', '.raw', '.tiff', '.svg'}
    video_exts = {'.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv', '.webm', '.m4v', '.3gp'}
    audio_exts = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a'}
    doc_exts = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.odt'}
    data_exts = {'.json', '.xml', '.csv', '.html', '.htm'}

    if ext in image_exts:
        return 'image'
    elif ext in video_exts:
        return 'video'
    elif ext in audio_exts:
        return 'audio'
    elif ext in doc_exts:
        return 'document'
    elif ext in data_exts:
        return 'data'
    else:
        return 'other'


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def create_html_report(
    hash_map: Dict[str, List[ZipFileInfo]],
    file_dates: Dict[ZipFileInfo, datetime],
    proposed_locations: Dict[ZipFileInfo, str],
    output_file: str = "takeout_report.html"
) -> Path:
    """
    Create an HTML report with file analysis results and interactive charts.

    Args:
        hash_map: Hash map from hasher.build_hash_map()
        file_dates: Dictionary mapping ZipFileInfo to their extracted dates
        proposed_locations: Dictionary mapping ZipFileInfo to proposed locations
        output_file: Output HTML filename

    Returns:
        Path to the created HTML file
    """
    # Gather all files for analysis
    all_files: List[ZipFileInfo] = []
    for files in hash_map.values():
        all_files.extend(files)

    # === BASIC STATISTICS ===
    total_files = len(all_files)
    unique_files = len(hash_map)
    duplicate_count = total_files - unique_files
    total_size = sum(f.file_size for f in all_files)
    duplicate_size = sum(
        sum(f.file_size for f in files[1:])
        for files in hash_map.values() if len(files) > 1
    )

    # === ZIP ARCHIVE ANALYSIS ===
    # First, identify which files are "originals" (first occurrence) vs duplicates
    original_files: set = set()  # Files that are the first occurrence of their content
    duplicate_files_set: set = set()  # Files that are duplicates of something else

    for content_key, file_list in hash_map.items():
        if len(file_list) >= 1:
            # First file is the "original"
            original_files.add(file_list[0])
            # Rest are duplicates
            for f in file_list[1:]:
                duplicate_files_set.add(f)

    # Build zip stats with duplicate tracking
    zip_stats: Dict[Path, dict] = {}
    for f in all_files:
        if f.zip_path not in zip_stats:
            zip_mtime = datetime.fromtimestamp(f.zip_path.stat().st_mtime)
            zip_stats[f.zip_path] = {
                'path': f.zip_path,
                'name': f.zip_path.name,
                'download_date': zip_mtime,
                'file_count': 0,
                'total_size': 0,
                'duplicate_count': 0,
                'duplicate_size': 0,
                'unique_count': 0,
                'types': defaultdict(int),
                'content_keys': set()  # For detecting identical zips
            }
        zip_stats[f.zip_path]['file_count'] += 1
        zip_stats[f.zip_path]['total_size'] += f.file_size
        zip_stats[f.zip_path]['types'][get_file_type(f.file_path)] += 1
        zip_stats[f.zip_path]['content_keys'].add(f.get_content_key())

        if f in duplicate_files_set:
            zip_stats[f.zip_path]['duplicate_count'] += 1
            zip_stats[f.zip_path]['duplicate_size'] += f.file_size
        else:
            zip_stats[f.zip_path]['unique_count'] += 1

    # Calculate duplicate percentage for each zip
    for zs in zip_stats.values():
        if zs['file_count'] > 0:
            zs['duplicate_pct'] = (zs['duplicate_count'] / zs['file_count']) * 100
        else:
            zs['duplicate_pct'] = 0
        # Create a content signature for detecting identical zips
        zs['content_signature'] = hash(frozenset(zs['content_keys']))

    # Build mapping of files by zip path (for cleanup tab)
    files_by_zip: Dict[Path, List[ZipFileInfo]] = defaultdict(list)
    for f in all_files:
        files_by_zip[f.zip_path].append(f)

    # Find redundant zips (100% duplicates - safe to delete)
    redundant_zips = [zs for zs in zip_stats.values() if zs['duplicate_pct'] == 100]
    redundant_zips.sort(key=lambda x: x['total_size'], reverse=True)

    # Find identical zips (same content signature)
    sig_to_zips: Dict[int, list] = defaultdict(list)
    for zs in zip_stats.values():
        sig_to_zips[zs['content_signature']].append(zs)
    identical_zip_groups = [group for group in sig_to_zips.values() if len(group) > 1]

    # Sort archives by download date
    sorted_zips = sorted(zip_stats.values(), key=lambda x: x['download_date'], reverse=True)

    # === FILE TYPE ANALYSIS ===
    type_counts = defaultdict(int)
    type_sizes = defaultdict(int)
    for f in all_files:
        ft = get_file_type(f.file_path)
        type_counts[ft] += 1
        type_sizes[ft] += f.file_size

    # === TIMELINE ANALYSIS ===
    year_counts = defaultdict(int)
    year_sizes = defaultdict(int)
    year_unique_counts = defaultdict(int)
    year_unique_sizes = defaultdict(int)
    year_duplicate_counts = defaultdict(int)
    year_duplicate_sizes = defaultdict(int)
    month_counts = defaultdict(int)  # YYYY-MM -> count
    for file_info, date in file_dates.items():
        if date:
            year_counts[date.year] += 1
            year_sizes[date.year] += file_info.file_size
            month_key = f"{date.year}-{date.month:02d}"
            month_counts[month_key] += 1
            # Track unique vs duplicate by year
            if file_info in duplicate_files_set:
                year_duplicate_counts[date.year] += 1
                year_duplicate_sizes[date.year] += file_info.file_size
            else:
                year_unique_counts[date.year] += 1
                year_unique_sizes[date.year] += file_info.file_size

    # === DUPLICATE ANALYSIS BY TYPE ===
    dup_by_type = defaultdict(lambda: {'count': 0, 'size': 0})
    for files in hash_map.values():
        if len(files) > 1:
            ft = get_file_type(files[0].file_path)
            dup_by_type[ft]['count'] += len(files) - 1
            dup_by_type[ft]['size'] += sum(f.file_size for f in files[1:])

    # === LARGEST FILES ===
    largest_files = sorted(all_files, key=lambda f: f.file_size, reverse=True)[:20]

    # === BUILD DATA FOR CHARTS ===
    # Type distribution chart data
    type_chart_data = {
        'labels': list(type_counts.keys()),
        'counts': list(type_counts.values()),
        'sizes': [type_sizes[t] for t in type_counts.keys()],
        'colors': ['#3b82f6', '#ef4444', '#8b5cf6', '#f59e0b', '#10b981', '#6b7280']
    }

    # Year timeline chart data
    sorted_years = sorted(year_counts.keys())
    year_chart_data = {
        'labels': [str(y) for y in sorted_years],
        'counts': [year_counts[y] for y in sorted_years],
        'sizes': [year_sizes[y] / (1024*1024*1024) for y in sorted_years],  # GB
        'unique_counts': [year_unique_counts[y] for y in sorted_years],
        'duplicate_counts': [year_duplicate_counts[y] for y in sorted_years],
        'unique_sizes': [year_unique_sizes[y] / (1024*1024*1024) for y in sorted_years],  # GB
        'duplicate_sizes': [year_duplicate_sizes[y] / (1024*1024*1024) for y in sorted_years]  # GB
    }

    # Monthly timeline (last 24 months)
    sorted_months = sorted(month_counts.keys())[-24:]
    month_chart_data = {
        'labels': sorted_months,
        'counts': [month_counts[m] for m in sorted_months]
    }

    # Archive chart data
    archive_chart_data = {
        'labels': [z['name'][:30] for z in sorted_zips[:10]],
        'counts': [z['file_count'] for z in sorted_zips[:10]],
        'sizes': [z['total_size'] / (1024*1024) for z in sorted_zips[:10]]  # MB
    }

    # Build file rows for table
    rows = []
    for file_hash, file_infos in hash_map.items():
        is_first = True
        for file_info in file_infos:
            file_date = file_dates.get(file_info)
            proposed = proposed_locations.get(file_info, "")
            file_type = get_file_type(file_info.file_path)
            zip_download = datetime.fromtimestamp(file_info.zip_path.stat().st_mtime)

            rows.append({
                'zip_name': file_info.zip_path.name,
                'file_path': file_info.file_path,
                'file_name': Path(file_info.file_path).name,
                'file_hash': file_hash[:12] + '...' if len(file_hash) > 12 else file_hash,
                'full_hash': file_hash,
                'is_duplicate': not is_first,
                'duplicate_of': file_infos[0].get_display_path() if not is_first else '',
                'date': file_date.strftime("%Y-%m-%d %H:%M") if file_date else 'Unknown',
                'year': file_date.year if file_date else 'Unknown',
                'download_date': zip_download.strftime("%Y-%m-%d"),
                'proposed_location': proposed,
                'file_type': file_type,
                'file_size': file_info.file_size,
                'file_size_display': format_file_size(file_info.file_size)
            })
            is_first = False

    rows.sort(key=lambda r: r['date'], reverse=True)

    # Generate HTML
    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Google Takeout Analysis Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            line-height: 1.6;
        }}
        .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}

        header {{
            background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%);
            border: 1px solid #334155;
            padding: 30px;
            margin-bottom: 30px;
            border-radius: 16px;
        }}
        header h1 {{ font-size: 2.5rem; margin-bottom: 8px; color: #fff; }}
        header p {{ color: #94a3b8; font-size: 1rem; }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
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
        .stat-card h3 {{ font-size: 2rem; color: #60a5fa; margin-bottom: 4px; }}
        .stat-card p {{ color: #94a3b8; font-size: 0.85rem; }}
        .stat-card.warning h3 {{ color: #fbbf24; }}
        .stat-card.success h3 {{ color: #34d399; }}
        .stat-card.danger h3 {{ color: #f87171; }}

        .section {{ margin-bottom: 30px; }}
        .section-title {{
            font-size: 1.3rem;
            color: #fff;
            margin-bottom: 16px;
            padding-bottom: 8px;
            border-bottom: 1px solid #334155;
        }}

        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .chart-card {{
            background: #1e293b;
            border: 1px solid #334155;
            padding: 20px;
            border-radius: 12px;
        }}
        .chart-card h4 {{ color: #fff; margin-bottom: 16px; font-size: 1.1rem; }}
        .chart-container {{ position: relative; height: 300px; }}

        .archive-list {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            overflow: hidden;
        }}
        .archive-item {{
            display: grid;
            grid-template-columns: 1fr auto auto auto;
            gap: 20px;
            padding: 16px 20px;
            border-bottom: 1px solid #334155;
            align-items: center;
        }}
        .archive-item:last-child {{ border-bottom: none; }}
        .archive-item:hover {{ background: #334155; }}
        .archive-name {{ font-weight: 500; color: #fff; }}
        .archive-date {{ color: #94a3b8; font-size: 0.9rem; }}
        .archive-count {{ color: #60a5fa; }}
        .archive-size {{ color: #34d399; font-family: monospace; }}

        .top-files {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            overflow: hidden;
        }}
        .top-file-item {{
            display: grid;
            grid-template-columns: 1fr auto auto;
            gap: 20px;
            padding: 12px 20px;
            border-bottom: 1px solid #334155;
            align-items: center;
        }}
        .top-file-item:hover {{ background: #334155; }}
        .top-file-name {{
            color: #fff;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        .top-file-type {{ color: #94a3b8; font-size: 0.85rem; }}
        .top-file-size {{ color: #fbbf24; font-family: monospace; font-weight: 600; }}

        .controls {{
            background: #1e293b;
            border: 1px solid #334155;
            padding: 16px 20px;
            border-radius: 12px;
            margin-bottom: 20px;
            display: flex;
            gap: 16px;
            flex-wrap: wrap;
            align-items: center;
        }}
        .controls input[type="text"] {{
            padding: 10px 14px;
            border: 1px solid #475569;
            border-radius: 8px;
            font-size: 0.9rem;
            min-width: 280px;
            background: #0f172a;
            color: #fff;
        }}
        .controls select {{
            padding: 10px 14px;
            border: 1px solid #475569;
            border-radius: 8px;
            font-size: 0.9rem;
            background: #0f172a;
            color: #fff;
        }}
        .controls label {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.9rem;
            color: #94a3b8;
        }}

        .table-container {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            overflow: hidden;
        }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{
            background: #0f172a;
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            color: #94a3b8;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            cursor: pointer;
            user-select: none;
        }}
        th:hover {{ background: #1e293b; }}
        td {{
            padding: 12px 16px;
            border-bottom: 1px solid #334155;
            font-size: 0.9rem;
            color: #e2e8f0;
        }}
        tr:hover {{ background: #334155; }}
        tr.duplicate {{ background: rgba(251, 191, 36, 0.1); }}
        tr.duplicate:hover {{ background: rgba(251, 191, 36, 0.2); }}

        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .badge.duplicate {{ background: #422006; color: #fbbf24; }}
        .badge.unique {{ background: #052e16; color: #34d399; }}
        .badge.image {{ background: #1e3a5f; color: #60a5fa; }}
        .badge.video {{ background: #450a0a; color: #f87171; }}
        .badge.audio {{ background: #2e1065; color: #a78bfa; }}
        .badge.document {{ background: #422006; color: #fbbf24; }}
        .badge.data {{ background: #052e16; color: #34d399; }}
        .badge.other {{ background: #1f2937; color: #9ca3af; }}

        .file-path {{
            max-width: 280px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .hash {{ font-family: monospace; font-size: 0.8rem; color: #64748b; }}
        .hidden {{ display: none !important; }}
        .results-count {{
            padding: 12px 16px;
            background: #0f172a;
            color: #94a3b8;
            font-size: 0.9rem;
        }}

        footer {{
            text-align: center;
            padding: 30px;
            color: #64748b;
            font-size: 0.85rem;
        }}

        .tabs {{
            display: flex;
            gap: 4px;
            margin-bottom: 20px;
            background: #1e293b;
            padding: 6px;
            border-radius: 10px;
            border: 1px solid #334155;
        }}
        .tab {{
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            color: #94a3b8;
            font-weight: 500;
            transition: all 0.2s;
        }}
        .tab:hover {{ background: #334155; color: #fff; }}
        .tab.active {{ background: #3b82f6; color: #fff; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 Google Takeout Analysis</h1>
            <p>Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")} • Analyzed {len(zip_stats)} archives</p>
        </header>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>{total_files:,}</h3>
                <p>Total Files</p>
            </div>
            <div class="stat-card success">
                <h3>{unique_files:,}</h3>
                <p>Unique Files</p>
            </div>
            <div class="stat-card warning">
                <h3>{duplicate_count:,}</h3>
                <p>Duplicates</p>
            </div>
            <div class="stat-card">
                <h3>{format_file_size(total_size)}</h3>
                <p>Total Size</p>
            </div>
            <div class="stat-card danger">
                <h3>{format_file_size(duplicate_size)}</h3>
                <p>Wasted Space</p>
            </div>
            <div class="stat-card">
                <h3>{len(zip_stats)}</h3>
                <p>Zip Archives</p>
            </div>
        </div>

        <div class="tabs">
            <div class="tab active" onclick="showTab('overview')">Overview</div>
            <div class="tab" onclick="showTab('archives')">Archives</div>
            <div class="tab" onclick="showTab('timeline')">Timeline</div>
            <div class="tab" onclick="showTab('cleanup')" style="color: #f87171;">🗑️ Cleanup</div>
            <div class="tab" onclick="showTab('files')">All Files</div>
        </div>

        <!-- OVERVIEW TAB -->
        <div id="overview" class="tab-content active">
            <div class="charts-grid">
                <div class="chart-card">
                    <h4>Files by Type</h4>
                    <div class="chart-container">
                        <canvas id="typeChart"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h4>Storage by Type</h4>
                    <div class="chart-container">
                        <canvas id="sizeChart"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h4>Duplicates by Type</h4>
                    <div class="chart-container">
                        <canvas id="dupChart"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h4>Files by Year</h4>
                    <div class="chart-container">
                        <canvas id="yearChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="section">
                <h3 class="section-title">📦 Largest Files</h3>
                <div class="top-files">
                    {generate_largest_files(largest_files)}
                </div>
            </div>
        </div>

        <!-- ARCHIVES TAB -->
        <div id="archives" class="tab-content">
            {generate_redundant_zips_section(redundant_zips, identical_zip_groups)}

            <div class="charts-grid">
                <div class="chart-card">
                    <h4>Files per Archive</h4>
                    <div class="chart-container">
                        <canvas id="archiveChart"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h4>Download Timeline</h4>
                    <div class="chart-container">
                        <canvas id="downloadChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="section">
                <h3 class="section-title">📁 All Archives (by download date)</h3>
                <div class="archive-list">
                    {generate_archive_list_with_duplicates(sorted_zips)}
                </div>
            </div>
        </div>

        <!-- TIMELINE TAB -->
        <div id="timeline" class="tab-content">
            <div class="charts-grid">
                <div class="chart-card" style="grid-column: span 2;">
                    <h4>Monthly Activity (Last 24 Months)</h4>
                    <div class="chart-container" style="height: 400px;">
                        <canvas id="monthlyChart"></canvas>
                    </div>
                </div>
            </div>
            <div class="charts-grid">
                <div class="chart-card">
                    <h4>Yearly File Count</h4>
                    <div class="chart-container">
                        <canvas id="yearlyCountChart"></canvas>
                    </div>
                </div>
                <div class="chart-card">
                    <h4>Yearly Storage (GB)</h4>
                    <div class="chart-container">
                        <canvas id="yearlySizeChart"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- CLEANUP TAB -->
        <div id="cleanup" class="tab-content">
            {generate_cleanup_tab(redundant_zips, identical_zip_groups, files_by_zip, hash_map, duplicate_files_set)}
        </div>

        <!-- FILES TAB -->
        <div id="files" class="tab-content">
            <div class="controls">
                <input type="text" id="searchInput" placeholder="Search files..." onkeyup="applyFilters()">
                <select id="typeFilter" onchange="applyFilters()">
                    <option value="">All Types</option>
                    <option value="image">Images</option>
                    <option value="video">Videos</option>
                    <option value="audio">Audio</option>
                    <option value="document">Documents</option>
                    <option value="data">Data Files</option>
                    <option value="other">Other</option>
                </select>
                <select id="duplicateFilter" onchange="applyFilters()">
                    <option value="">All Files</option>
                    <option value="unique">Unique Only</option>
                    <option value="duplicate">Duplicates Only</option>
                </select>
                <select id="pageSizeSelect" onchange="changePageSize()">
                    <option value="100">100 per page</option>
                    <option value="250">250 per page</option>
                    <option value="500">500 per page</option>
                    <option value="1000">1000 per page</option>
                </select>
            </div>

            <div class="table-container">
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 12px 16px; background: #0f172a;">
                    <div class="results-count" id="resultsCount">Loading...</div>
                    <div id="paginationControls" style="display: flex; gap: 8px; align-items: center;">
                        <button onclick="prevPage()" id="prevBtn" style="padding: 6px 12px; background: #334155; border: none; color: #fff; border-radius: 4px; cursor: pointer;">← Prev</button>
                        <span id="pageInfo" style="color: #94a3b8;">Page 1</span>
                        <button onclick="nextPage()" id="nextBtn" style="padding: 6px 12px; background: #334155; border: none; color: #fff; border-radius: 4px; cursor: pointer;">Next →</button>
                    </div>
                </div>
                <table id="fileTable">
                    <thead>
                        <tr>
                            <th onclick="sortBy('file_name')">File Name</th>
                            <th onclick="sortBy('file_type')">Type</th>
                            <th onclick="sortBy('file_size')">Size</th>
                            <th onclick="sortBy('date')">File Date</th>
                            <th onclick="sortBy('download_date')">Downloaded</th>
                            <th onclick="sortBy('is_duplicate')">Status</th>
                            <th onclick="sortBy('duplicate_of')">Duplicate Of</th>
                            <th onclick="sortBy('zip_name')">Archive</th>
                        </tr>
                    </thead>
                    <tbody id="tableBody">
                    </tbody>
                </table>
            </div>
        </div>

        <footer>
            <p>Google Takeout Tool • Analyzed {total_files:,} files across {len(zip_stats)} archives</p>
        </footer>
    </div>

    <script>
        // Tab switching
        function showTab(tabId) {{
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById(tabId).classList.add('active');
            event.target.classList.add('active');
            // Initialize table when Files tab is shown
            if (tabId === 'files' && !tableInitialized) {{
                tableInitialized = true;
                renderTable();
            }}
        }}

        // === FILE TABLE DATA AND PAGINATION ===
        const allFiles = {json.dumps(rows)};
        let filteredFiles = [...allFiles];
        let currentPage = 0;
        let pageSize = 100;
        let sortColumn = 'date';
        let sortAsc = false;
        let tableInitialized = false;

        function formatFileSize(bytes) {{
            const units = ['B', 'KB', 'MB', 'GB'];
            let size = bytes;
            for (const unit of units) {{
                if (size < 1024) return size.toFixed(1) + ' ' + unit;
                size /= 1024;
            }}
            return size.toFixed(1) + ' TB';
        }}

        function applyFilters() {{
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            const typeFilter = document.getElementById('typeFilter').value;
            const duplicateFilter = document.getElementById('duplicateFilter').value;

            filteredFiles = allFiles.filter(row => {{
                if (searchTerm && !row.file_name.toLowerCase().includes(searchTerm)) return false;
                if (typeFilter && row.file_type !== typeFilter) return false;
                if (duplicateFilter === 'unique' && row.is_duplicate) return false;
                if (duplicateFilter === 'duplicate' && !row.is_duplicate) return false;
                return true;
            }});

            currentPage = 0;
            renderTable();
        }}

        function sortBy(column) {{
            if (sortColumn === column) {{
                sortAsc = !sortAsc;
            }} else {{
                sortColumn = column;
                sortAsc = true;
            }}

            filteredFiles.sort((a, b) => {{
                let aVal = a[column];
                let bVal = b[column];
                if (column === 'file_size') {{
                    return sortAsc ? aVal - bVal : bVal - aVal;
                }}
                if (typeof aVal === 'boolean') {{
                    return sortAsc ? (aVal ? 1 : -1) - (bVal ? 1 : -1) : (bVal ? 1 : -1) - (aVal ? 1 : -1);
                }}
                aVal = String(aVal || '');
                bVal = String(bVal || '');
                return sortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
            }});

            renderTable();
        }}

        function changePageSize() {{
            pageSize = parseInt(document.getElementById('pageSizeSelect').value);
            currentPage = 0;
            renderTable();
        }}

        function prevPage() {{
            if (currentPage > 0) {{
                currentPage--;
                renderTable();
            }}
        }}

        function nextPage() {{
            const maxPage = Math.ceil(filteredFiles.length / pageSize) - 1;
            if (currentPage < maxPage) {{
                currentPage++;
                renderTable();
            }}
        }}

        function renderTable() {{
            const tbody = document.getElementById('tableBody');
            const start = currentPage * pageSize;
            const end = Math.min(start + pageSize, filteredFiles.length);
            const pageData = filteredFiles.slice(start, end);
            const totalPages = Math.ceil(filteredFiles.length / pageSize);

            // Update counts and pagination
            document.getElementById('resultsCount').textContent =
                `Showing ${{start + 1}}-${{end}} of ${{filteredFiles.length.toLocaleString()}} files`;
            document.getElementById('pageInfo').textContent =
                `Page ${{currentPage + 1}} of ${{totalPages}}`;
            document.getElementById('prevBtn').disabled = currentPage === 0;
            document.getElementById('nextBtn').disabled = currentPage >= totalPages - 1;

            // Render rows
            tbody.innerHTML = pageData.map(row => {{
                const dupClass = row.is_duplicate ? 'duplicate' : '';
                const statusBadge = row.is_duplicate
                    ? '<span class="badge duplicate">Duplicate</span>'
                    : '<span class="badge unique">Unique</span>';
                const dupOf = row.is_duplicate ? row.duplicate_of : '-';

                return `<tr class="${{dupClass}}" title="${{row.file_path}}">
                    <td class="file-path">${{row.file_name}}</td>
                    <td><span class="badge ${{row.file_type}}">${{row.file_type}}</span></td>
                    <td>${{row.file_size_display}}</td>
                    <td>${{row.date}}</td>
                    <td>${{row.download_date}}</td>
                    <td>${{statusBadge}}</td>
                    <td class="file-path" style="max-width: 200px; color: #94a3b8; font-size: 0.85rem;">${{dupOf}}</td>
                    <td>${{row.zip_name}}</td>
                </tr>`;
            }}).join('');
        }}

        // Chart.js defaults
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.borderColor = '#334155';

        const chartColors = ['#3b82f6', '#ef4444', '#8b5cf6', '#f59e0b', '#10b981', '#6b7280'];

        // Type Distribution Pie Chart
        new Chart(document.getElementById('typeChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(type_chart_data['labels'])},
                datasets: [{{
                    data: {json.dumps(type_chart_data['counts'])},
                    backgroundColor: chartColors
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'right' }} }}
            }}
        }});

        // Size by Type Chart
        new Chart(document.getElementById('sizeChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(type_chart_data['labels'])},
                datasets: [{{
                    data: {json.dumps([round(s/(1024*1024*1024), 2) for s in type_chart_data['sizes']])},
                    backgroundColor: chartColors
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ position: 'right' }},
                    tooltip: {{
                        callbacks: {{
                            label: ctx => ctx.label + ': ' + ctx.raw.toFixed(2) + ' GB'
                        }}
                    }}
                }}
            }}
        }});

        // Duplicates by Type
        new Chart(document.getElementById('dupChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(list(dup_by_type.keys()))},
                datasets: [{{
                    label: 'Duplicate Count',
                    data: {json.dumps([d['count'] for d in dup_by_type.values()])},
                    backgroundColor: '#fbbf24'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }}
            }}
        }});

        // Year Bar Chart (stacked unique + duplicates)
        new Chart(document.getElementById('yearChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(year_chart_data['labels'])},
                datasets: [{{
                    label: 'Unique',
                    data: {json.dumps(year_chart_data['unique_counts'])},
                    backgroundColor: '#3b82f6'
                }}, {{
                    label: 'Duplicates',
                    data: {json.dumps(year_chart_data['duplicate_counts'])},
                    backgroundColor: '#fbbf24'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }},
                plugins: {{ legend: {{ display: true, position: 'top' }} }}
            }}
        }});

        // Archive Chart
        new Chart(document.getElementById('archiveChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(archive_chart_data['labels'])},
                datasets: [{{
                    label: 'Files',
                    data: {json.dumps(archive_chart_data['counts'])},
                    backgroundColor: '#8b5cf6'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: 'y',
                plugins: {{ legend: {{ display: false }} }}
            }}
        }});

        // Download Timeline
        const downloadDates = {json.dumps([z['download_date'].strftime('%Y-%m-%d') for z in sorted_zips])};
        const downloadCounts = {json.dumps([z['file_count'] for z in sorted_zips])};
        new Chart(document.getElementById('downloadChart'), {{
            type: 'scatter',
            data: {{
                datasets: [{{
                    label: 'Archive Downloads',
                    data: downloadDates.map((d, i) => ({{ x: d, y: downloadCounts[i] }})),
                    backgroundColor: '#10b981',
                    pointRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{ x: {{ type: 'category' }} }},
                plugins: {{ legend: {{ display: false }} }}
            }}
        }});

        // Monthly Chart
        new Chart(document.getElementById('monthlyChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(month_chart_data['labels'])},
                datasets: [{{
                    label: 'Files per Month',
                    data: {json.dumps(month_chart_data['counts'])},
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    fill: true,
                    tension: 0.3
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }}
            }}
        }});

        // Yearly Count Chart (stacked bar)
        new Chart(document.getElementById('yearlyCountChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(year_chart_data['labels'])},
                datasets: [{{
                    label: 'Unique Files',
                    data: {json.dumps(year_chart_data['unique_counts'])},
                    backgroundColor: '#10b981'
                }}, {{
                    label: 'Duplicates',
                    data: {json.dumps(year_chart_data['duplicate_counts'])},
                    backgroundColor: '#fbbf24'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }},
                plugins: {{ legend: {{ display: true, position: 'top' }} }}
            }}
        }});

        // Yearly Size Chart (stacked bar)
        new Chart(document.getElementById('yearlySizeChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(year_chart_data['labels'])},
                datasets: [{{
                    label: 'Unique (GB)',
                    data: {json.dumps([round(s, 2) for s in year_chart_data['unique_sizes']])},
                    backgroundColor: '#f59e0b'
                }}, {{
                    label: 'Duplicates (GB)',
                    data: {json.dumps([round(s, 2) for s in year_chart_data['duplicate_sizes']])},
                    backgroundColor: '#ef4444'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }},
                plugins: {{ legend: {{ display: true, position: 'top' }} }}
            }}
        }});

    </script>
</body>
</html>'''

    output_path = Path(output_file)
    console.print(f"[cyan]Writing HTML report to: {output_path.absolute()}[/cyan]")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    console.print(f"[green]HTML report created with {len(rows)} entries[/green]")
    return output_path


def generate_largest_files(files: List[ZipFileInfo]) -> str:
    """Generate HTML for largest files list."""
    items = []
    for f in files:
        items.append(f'''
            <div class="top-file-item">
                <div class="top-file-name" title="{f.file_path}">{Path(f.file_path).name}</div>
                <div class="top-file-type">{get_file_type(f.file_path)}</div>
                <div class="top-file-size">{format_file_size(f.file_size)}</div>
            </div>
        ''')
    return '\n'.join(items)


def generate_archive_list(archives: List[dict]) -> str:
    """Generate HTML for archive list."""
    items = []
    for arch in archives:
        items.append(f'''
            <div class="archive-item">
                <div class="archive-name">{arch['name']}</div>
                <div class="archive-date">Downloaded: {arch['download_date'].strftime('%Y-%m-%d %H:%M')}</div>
                <div class="archive-count">{arch['file_count']:,} files</div>
                <div class="archive-size">{format_file_size(arch['total_size'])}</div>
            </div>
        ''')
    return '\n'.join(items)


def generate_archive_list_with_duplicates(archives: List[dict]) -> str:
    """Generate HTML for archive list with duplicate percentages."""
    items = []
    for arch in archives:
        dup_pct = arch.get('duplicate_pct', 0)
        dup_size = arch.get('duplicate_size', 0)

        # Color code based on duplicate percentage
        if dup_pct == 100:
            pct_class = 'danger'
            status = '🗑️ REDUNDANT'
        elif dup_pct >= 80:
            pct_class = 'warning'
            status = '⚠️ Mostly duplicates'
        elif dup_pct >= 50:
            pct_class = 'warning'
            status = ''
        else:
            pct_class = 'success'
            status = ''

        items.append(f'''
            <div class="archive-item" style="{'background: rgba(239, 68, 68, 0.1);' if dup_pct == 100 else ''}">
                <div class="archive-name">
                    {arch['name']}
                    {f'<span style="color: #f87171; font-size: 0.8rem; margin-left: 8px;">{status}</span>' if status else ''}
                </div>
                <div class="archive-date">Downloaded: {arch['download_date'].strftime('%Y-%m-%d %H:%M')}</div>
                <div class="archive-count">{arch['file_count']:,} files</div>
                <div style="color: {'#f87171' if pct_class == 'danger' else '#fbbf24' if pct_class == 'warning' else '#34d399'};">
                    {dup_pct:.0f}% dups ({format_file_size(dup_size)})
                </div>
                <div class="archive-size">{format_file_size(arch['total_size'])}</div>
            </div>
        ''')
    return '\n'.join(items)


def generate_redundant_zips_section(redundant_zips: List[dict], identical_groups: List[List[dict]]) -> str:
    """Generate HTML section for redundant and identical zips."""
    if not redundant_zips and not identical_groups:
        return ''

    html_parts = []

    # Redundant zips (100% duplicates)
    if redundant_zips:
        total_wasted = sum(z['total_size'] for z in redundant_zips)
        html_parts.append(f'''
            <div class="section">
                <h3 class="section-title" style="color: #f87171;">🗑️ Redundant Archives ({len(redundant_zips)} zips, {format_file_size(total_wasted)} wasted)</h3>
                <p style="color: #94a3b8; margin-bottom: 16px;">These archives contain ONLY files that exist in other archives. They can be safely deleted.</p>
                <div class="archive-list" style="border-color: #ef4444;">
        ''')
        for z in redundant_zips:
            html_parts.append(f'''
                    <div class="archive-item" style="background: rgba(239, 68, 68, 0.1);">
                        <div class="archive-name" style="color: #f87171;">{z['name']}</div>
                        <div class="archive-date">Downloaded: {z['download_date'].strftime('%Y-%m-%d %H:%M')}</div>
                        <div class="archive-count">{z['file_count']:,} duplicate files</div>
                        <div class="archive-size" style="color: #f87171;">{format_file_size(z['total_size'])} wasted</div>
                    </div>
            ''')
        html_parts.append('</div></div>')

    # Identical zip groups
    if identical_groups:
        html_parts.append(f'''
            <div class="section">
                <h3 class="section-title" style="color: #fbbf24;">📋 Identical Archives ({len(identical_groups)} groups)</h3>
                <p style="color: #94a3b8; margin-bottom: 16px;">These groups of archives have identical contents. Keep one from each group.</p>
        ''')
        for i, group in enumerate(identical_groups):
            group_size = group[0]['total_size']
            wasted = group_size * (len(group) - 1)
            html_parts.append(f'''
                <div style="margin-bottom: 16px;">
                    <h4 style="color: #fbbf24; margin-bottom: 8px;">Group {i+1}: {len(group)} identical zips ({format_file_size(wasted)} wasted)</h4>
                    <div class="archive-list" style="border-color: #fbbf24;">
            ''')
            for j, z in enumerate(sorted(group, key=lambda x: x['download_date'])):
                is_oldest = j == 0
                html_parts.append(f'''
                        <div class="archive-item" style="{'background: rgba(52, 211, 153, 0.1);' if is_oldest else 'background: rgba(251, 191, 36, 0.1);'}">
                            <div class="archive-name">
                                {z['name']}
                                {'<span style="color: #34d399; font-size: 0.8rem; margin-left: 8px;">✓ KEEP (oldest)</span>' if is_oldest else '<span style="color: #fbbf24; font-size: 0.8rem; margin-left: 8px;">can delete</span>'}
                            </div>
                            <div class="archive-date">Downloaded: {z['download_date'].strftime('%Y-%m-%d %H:%M')}</div>
                            <div class="archive-count">{z['file_count']:,} files</div>
                            <div class="archive-size">{format_file_size(z['total_size'])}</div>
                        </div>
                ''')
            html_parts.append('</div></div>')
        html_parts.append('</div>')

    return '\n'.join(html_parts)


def generate_cleanup_tab(
    redundant_zips: List[dict],
    identical_groups: List[List[dict]],
    files_by_zip: Dict[Path, List[ZipFileInfo]],
    hash_map: Dict[str, List[ZipFileInfo]],
    duplicate_files: set
) -> str:
    """Generate HTML content for the Cleanup tab with detailed file listings."""

    # Build reverse lookup: duplicate file -> original file
    duplicate_to_original: Dict[ZipFileInfo, ZipFileInfo] = {}
    for content_key, file_list in hash_map.items():
        if len(file_list) > 1:
            original = file_list[0]
            for dup in file_list[1:]:
                duplicate_to_original[dup] = original

    html_parts = []

    # Summary stats
    total_redundant = len(redundant_zips)
    total_wasted = sum(z['total_size'] for z in redundant_zips)
    identical_wasted = sum(
        g[0]['total_size'] * (len(g) - 1) for g in identical_groups
    ) if identical_groups else 0

    html_parts.append(f'''
        <div class="stats-grid" style="margin-bottom: 30px;">
            <div class="stat-card danger">
                <h3>{total_redundant}</h3>
                <p>Redundant Archives</p>
            </div>
            <div class="stat-card danger">
                <h3>{format_file_size(total_wasted)}</h3>
                <p>Space to Recover</p>
            </div>
            <div class="stat-card warning">
                <h3>{len(identical_groups)}</h3>
                <p>Identical Groups</p>
            </div>
            <div class="stat-card warning">
                <h3>{format_file_size(identical_wasted)}</h3>
                <p>Identical Waste</p>
            </div>
        </div>
    ''')

    if not redundant_zips and not identical_groups:
        html_parts.append('''
            <div style="text-align: center; padding: 60px; color: #34d399;">
                <h2>✨ No cleanup needed!</h2>
                <p style="color: #94a3b8; margin-top: 16px;">All your archives contain unique content.</p>
            </div>
        ''')
        return '\n'.join(html_parts)

    # Redundant zips with file listings
    if redundant_zips:
        html_parts.append(f'''
            <div class="section">
                <h3 class="section-title" style="color: #f87171;">
                    🗑️ Redundant Archives - Safe to Delete ({len(redundant_zips)} zips, {format_file_size(total_wasted)})
                </h3>
                <p style="color: #94a3b8; margin-bottom: 20px;">
                    These archives contain ONLY files that already exist in other archives.
                    All files are duplicates - you can safely delete these entire zip files.
                </p>
        ''')

        for z in redundant_zips:
            zip_path = z['path']
            zip_files = files_by_zip.get(zip_path, [])

            # Group files by what they're duplicates of
            originals_map: Dict[Path, List[tuple]] = defaultdict(list)
            for f in zip_files:
                if f in duplicate_to_original:
                    orig = duplicate_to_original[f]
                    originals_map[orig.zip_path].append((f, orig))

            html_parts.append(f'''
                <div style="background: #1e293b; border: 1px solid #ef4444; border-radius: 12px; margin-bottom: 20px; overflow: hidden;">
                    <div style="background: rgba(239, 68, 68, 0.2); padding: 16px 20px; display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="color: #f87171; font-weight: 600; font-size: 1.1rem;">{z['name']}</span>
                            <span style="color: #94a3b8; margin-left: 16px;">Downloaded: {z['download_date'].strftime('%Y-%m-%d')}</span>
                        </div>
                        <div style="text-align: right;">
                            <span style="color: #f87171; font-weight: 600;">{z['file_count']:,} files</span>
                            <span style="color: #94a3b8; margin-left: 8px;">({format_file_size(z['total_size'])})</span>
                        </div>
                    </div>
                    <div style="padding: 16px 20px;">
                        <p style="color: #94a3b8; margin-bottom: 12px; font-size: 0.9rem;">
                            Files duplicated from:
                        </p>
            ''')

            # Show which archives have the originals
            for orig_zip, file_pairs in sorted(originals_map.items(), key=lambda x: len(x[1]), reverse=True):
                html_parts.append(f'''
                        <div style="background: #0f172a; border-radius: 8px; padding: 12px 16px; margin-bottom: 8px;">
                            <div style="color: #34d399; font-weight: 500; margin-bottom: 8px;">
                                ✓ {orig_zip.name} ({len(file_pairs)} files)
                            </div>
                            <div style="display: flex; flex-wrap: wrap; gap: 8px; max-height: 100px; overflow-y: auto;">
                ''')
                for dup_file, orig_file in file_pairs[:10]:  # Show first 10
                    html_parts.append(f'''
                                <span style="background: #334155; padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; color: #e2e8f0;">
                                    {Path(dup_file.file_path).name}
                                </span>
                    ''')
                if len(file_pairs) > 10:
                    html_parts.append(f'''
                                <span style="color: #94a3b8; font-size: 0.8rem; padding: 4px;">
                                    +{len(file_pairs) - 10} more...
                                </span>
                    ''')
                html_parts.append('</div></div>')

            html_parts.append('</div></div>')

        html_parts.append('</div>')

    # Identical zip groups
    if identical_groups:
        html_parts.append(f'''
            <div class="section">
                <h3 class="section-title" style="color: #fbbf24;">
                    📋 Identical Archives - Keep One Per Group ({len(identical_groups)} groups)
                </h3>
                <p style="color: #94a3b8; margin-bottom: 20px;">
                    These groups contain archives with exactly the same content.
                    Keep the oldest one (marked green) and delete the rest.
                </p>
        ''')

        for i, group in enumerate(identical_groups):
            group_size = group[0]['total_size']
            wasted = group_size * (len(group) - 1)
            sorted_group = sorted(group, key=lambda x: x['download_date'])

            html_parts.append(f'''
                <div style="background: #1e293b; border: 1px solid #fbbf24; border-radius: 12px; margin-bottom: 20px; overflow: hidden;">
                    <div style="background: rgba(251, 191, 36, 0.2); padding: 16px 20px; display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="color: #fbbf24; font-weight: 600;">Group {i+1}</span>
                            <span style="color: #94a3b8; margin-left: 16px;">{len(group)} identical archives</span>
                        </div>
                        <div>
                            <span style="color: #fbbf24;">{format_file_size(wasted)} wasted</span>
                        </div>
                    </div>
                    <div style="padding: 0;">
            ''')

            for j, z in enumerate(sorted_group):
                is_keep = j == 0
                html_parts.append(f'''
                        <div style="display: flex; justify-content: space-between; align-items: center; padding: 12px 20px; border-bottom: 1px solid #334155; background: {'rgba(52, 211, 153, 0.1)' if is_keep else 'transparent'};">
                            <div>
                                <span style="color: {'#34d399' if is_keep else '#e2e8f0'}; font-weight: 500;">{z['name']}</span>
                                {'<span style="background: #052e16; color: #34d399; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; margin-left: 8px;">KEEP</span>' if is_keep else '<span style="background: #422006; color: #fbbf24; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; margin-left: 8px;">DELETE</span>'}
                            </div>
                            <div style="color: #94a3b8; font-size: 0.9rem;">
                                {z['download_date'].strftime('%Y-%m-%d %H:%M')} • {z['file_count']:,} files • {format_file_size(z['total_size'])}
                            </div>
                        </div>
                ''')

            html_parts.append('</div></div>')

        html_parts.append('</div>')

    return '\n'.join(html_parts)


def open_html(html_path: Path) -> None:
    """Open the HTML file with the default browser."""
    try:
        webbrowser.open(html_path.absolute().as_uri())
        console.print(f"[green]Opening HTML report in browser...[/green]")
    except Exception as e:
        console.print(f"[yellow]Could not open report automatically: {e}[/yellow]")
        console.print(f"[yellow]Please open manually: {html_path.absolute()}[/yellow]")
