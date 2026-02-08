"""Cleanup module for managing source zip deletion after extraction."""
import sys
import zipfile
from enum import Enum
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from rich.console import Console

console = Console(legacy_windows=(sys.platform == "win32"))


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


class CleanupMode(Enum):
    """Mode for handling source zip cleanup."""
    PROMPT = "prompt"      # Ask for each zip
    AUTO_DELETE = "auto"   # Delete all after extraction
    KEEP_ALL = "keep"      # Never delete


@dataclass
class CleanupResult:
    """Result of a cleanup operation."""
    zip_path: Path
    deleted: bool
    size_freed: int
    error: Optional[str] = None


class CleanupManager:
    """Manages cleanup prompts and safe deletion of source zips."""

    def __init__(self, mode: CleanupMode = CleanupMode.PROMPT):
        """
        Initialize the cleanup manager.

        Args:
            mode: Initial cleanup mode
        """
        self.mode = mode
        self._delete_all = mode == CleanupMode.AUTO_DELETE
        self._keep_all = mode == CleanupMode.KEEP_ALL

    def prompt_cleanup(
        self,
        zip_path: Path,
        extracted_count: int,
        error_count: int
    ) -> CleanupResult:
        """
        Prompt user about deleting a source zip after extraction.

        Args:
            zip_path: Path to the zip file
            extracted_count: Number of files successfully extracted
            error_count: Number of extraction errors

        Returns:
            CleanupResult indicating whether the file was deleted
        """
        # If user previously chose "Never", skip all prompts
        if self._keep_all:
            return CleanupResult(zip_path=zip_path, deleted=False, size_freed=0)

        # If user previously chose "All", delete without prompting
        if self._delete_all:
            return self.delete_zip(zip_path)

        # Get zip size for display
        try:
            zip_size = zip_path.stat().st_size
        except OSError:
            zip_size = 0

        # Don't prompt if there were errors
        if error_count > 0:
            console.print(
                f"[yellow]Keeping {zip_path.name} due to {error_count} extraction errors[/yellow]"
            )
            return CleanupResult(zip_path=zip_path, deleted=False, size_freed=0)

        # Interactive prompt
        console.print(f"\n[bold]Extracted {extracted_count} files from {zip_path.name}[/bold]")
        console.print(f"  Archive size: {format_size(zip_size)}")

        while True:
            try:
                response = console.input(
                    "[yellow]Delete original zip?[/yellow] [Y]es / [N]o / [A]ll / Ne[v]er: "
                ).strip().lower()

                if response in ('y', 'yes'):
                    return self.delete_zip(zip_path)

                elif response in ('n', 'no'):
                    console.print(f"[dim]Keeping {zip_path.name}[/dim]")
                    return CleanupResult(zip_path=zip_path, deleted=False, size_freed=0)

                elif response in ('a', 'all'):
                    self._delete_all = True
                    console.print("[cyan]Will delete all remaining zips after extraction[/cyan]")
                    return self.delete_zip(zip_path)

                elif response in ('v', 'never'):
                    self._keep_all = True
                    console.print("[cyan]Will keep all remaining zips[/cyan]")
                    return CleanupResult(zip_path=zip_path, deleted=False, size_freed=0)

                else:
                    console.print("[red]Invalid option. Please enter Y, N, A, or V[/red]")

            except KeyboardInterrupt:
                console.print("\n[yellow]Keeping remaining zips[/yellow]")
                self._keep_all = True
                return CleanupResult(zip_path=zip_path, deleted=False, size_freed=0)

    def delete_zip(self, zip_path: Path) -> CleanupResult:
        """
        Safely delete a zip file after verification.

        Args:
            zip_path: Path to the zip file to delete

        Returns:
            CleanupResult with deletion status
        """
        try:
            # Get size before deletion
            size = zip_path.stat().st_size

            # Verify it's actually a zip file before deleting
            if not zipfile.is_zipfile(zip_path):
                return CleanupResult(
                    zip_path=zip_path,
                    deleted=False,
                    size_freed=0,
                    error="Not a valid zip file - refusing to delete"
                )

            # Delete the file
            zip_path.unlink()
            console.print(f"[green]Deleted {zip_path.name} ({format_size(size)} freed)[/green]")

            return CleanupResult(
                zip_path=zip_path,
                deleted=True,
                size_freed=size
            )

        except PermissionError:
            error_msg = "Permission denied - file may be in use"
            console.print(f"[red]Cannot delete {zip_path.name}: {error_msg}[/red]")
            return CleanupResult(
                zip_path=zip_path,
                deleted=False,
                size_freed=0,
                error=error_msg
            )

        except FileNotFoundError:
            return CleanupResult(
                zip_path=zip_path,
                deleted=False,
                size_freed=0,
                error="File not found"
            )

        except Exception as e:
            error_msg = str(e)
            console.print(f"[red]Cannot delete {zip_path.name}: {error_msg}[/red]")
            return CleanupResult(
                zip_path=zip_path,
                deleted=False,
                size_freed=0,
                error=error_msg
            )
