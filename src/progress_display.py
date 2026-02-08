"""Progress display module for rich TUI progress tracking."""
import sys
import time
from dataclasses import dataclass, field
from typing import List, Optional
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn, TaskProgressColumn
from rich.table import Table
from rich.text import Text


console = Console(legacy_windows=(sys.platform == "win32"))


def format_size(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def truncate_path(path: str, max_length: int = 50) -> str:
    """Truncate a path string from the middle."""
    if len(path) <= max_length:
        return path
    half = (max_length - 3) // 2
    return path[:half] + "..." + path[-half:]


@dataclass
class ProgressStats:
    """Track progress statistics."""
    total_files: int = 0
    completed_files: int = 0
    total_bytes: int = 0
    processed_bytes: int = 0
    duplicates_found: int = 0
    unique_files: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    current_file: str = ""
    recent_files: List[str] = field(default_factory=list)
    max_recent: int = 5

    def add_completed(self, filename: str, file_size: int, is_duplicate: bool):
        """Record a completed file."""
        self.completed_files += 1
        self.processed_bytes += file_size

        if is_duplicate:
            self.duplicates_found += 1
        else:
            self.unique_files += 1

        self.recent_files.append(filename)
        if len(self.recent_files) > self.max_recent:
            self.recent_files.pop(0)

    def add_error(self, filename: str):
        """Record an error."""
        self.completed_files += 1
        self.errors += 1

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    @property
    def files_per_second(self) -> float:
        if self.elapsed == 0:
            return 0
        return self.completed_files / self.elapsed

    @property
    def bytes_per_second(self) -> float:
        if self.elapsed == 0:
            return 0
        return self.processed_bytes / self.elapsed

    @property
    def percent_complete(self) -> float:
        if self.total_files == 0:
            return 0
        return (self.completed_files / self.total_files) * 100

    @property
    def eta_seconds(self) -> Optional[float]:
        if self.files_per_second == 0:
            return None
        remaining = self.total_files - self.completed_files
        return remaining / self.files_per_second


class HashingProgressDisplay:
    """Rich TUI display for hashing progress."""

    def __init__(self, total_files: int, total_bytes: int, num_workers: int):
        self.stats = ProgressStats(total_files=total_files, total_bytes=total_bytes)
        self.num_workers = num_workers
        self.live: Optional[Live] = None
        self._last_refresh = 0.0
        self._min_refresh_interval = 0.1  # Max 10 updates per second

        # Create progress bar
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console,
            expand=False
        )
        self.task_id = self.progress.add_task("Hashing", total=total_files)

    def _create_display(self) -> Panel:
        """Create the display panel."""
        # Stats table
        stats_table = Table.grid(padding=(0, 2))
        stats_table.add_column(style="cyan", justify="right")
        stats_table.add_column(style="white")
        stats_table.add_column(style="cyan", justify="right")
        stats_table.add_column(style="white")

        stats_table.add_row(
            "Files:", f"{self.stats.completed_files:,} / {self.stats.total_files:,}",
            "Speed:", f"{self.stats.files_per_second:.1f} files/s"
        )
        stats_table.add_row(
            "Data:", f"{format_size(self.stats.processed_bytes)} / {format_size(self.stats.total_bytes)}",
            "Throughput:", f"{format_size(int(self.stats.bytes_per_second))}/s"
        )
        stats_table.add_row(
            "Unique:", f"{self.stats.unique_files:,}",
            "Duplicates:", f"[yellow]{self.stats.duplicates_found:,}[/yellow]"
        )
        if self.stats.errors > 0:
            stats_table.add_row(
                "Errors:", f"[red]{self.stats.errors:,}[/red]",
                "", ""
            )

        # Current file
        current_text = Text()
        current_text.append("Current: ", style="dim")
        current_text.append(truncate_path(self.stats.current_file, 60) if self.stats.current_file else "...", style="white")

        # Recent files
        recent_table = Table.grid(padding=(0, 1))
        recent_table.add_column(style="dim", width=3)
        recent_table.add_column(style="green")

        for i, filename in enumerate(reversed(self.stats.recent_files[-4:])):
            recent_table.add_row("✓", truncate_path(filename, 55))

        # Combine into groups
        content = Group(
            self.progress,
            Text(),
            stats_table,
            Text(),
            current_text,
            Text(),
            Text("Recently completed:", style="dim"),
            recent_table
        )

        return Panel(
            content,
            title=f"[bold]Hashing Files[/bold] [dim]({self.num_workers} workers)[/dim]",
            border_style="blue",
            padding=(1, 2)
        )

    def __enter__(self):
        self.progress.start()
        self.live = Live(
            self._create_display(),
            console=console,
            refresh_per_second=4,  # Low rate, we manually control updates
            transient=True
        )
        self.live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Final update to show completion state
        if self.live:
            self.live.update(self._create_display())
            self.live.__exit__(exc_type, exc_val, exc_tb)
        self.progress.stop()

    def _should_refresh(self) -> bool:
        """Check if enough time has passed for a display refresh."""
        now = time.time()
        if now - self._last_refresh >= self._min_refresh_interval:
            self._last_refresh = now
            return True
        return False

    def update(self, filename: str, file_size: int, is_duplicate: bool, is_error: bool = False):
        """Update progress with a completed file."""
        self.stats.current_file = filename

        if is_error:
            self.stats.add_error(filename)
        else:
            self.stats.add_completed(filename, file_size, is_duplicate)

        self.progress.update(self.task_id, completed=self.stats.completed_files)

        # Throttle display updates to prevent flickering
        if self.live and self._should_refresh():
            self.live.update(self._create_display())

    def set_current(self, filename: str):
        """Set the current file being processed."""
        self.stats.current_file = filename
        # Don't force refresh for current file updates


class SimpleProgressDisplay:
    """Simpler progress display for non-parallel operations."""

    def __init__(self, description: str, total: int):
        self.total = total
        self.completed = 0
        self.description = description
        self.start_time = time.time()

        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            TextColumn("[dim]{task.fields[status]}[/dim]"),
            TimeRemainingColumn(),
            console=console
        )
        self.task_id = self.progress.add_task(
            description,
            total=total,
            status=""
        )

    def __enter__(self):
        self.progress.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.stop()

    def update(self, status: str = ""):
        """Advance progress by one."""
        self.completed += 1
        self.progress.update(
            self.task_id,
            completed=self.completed,
            status=truncate_path(status, 40)
        )
