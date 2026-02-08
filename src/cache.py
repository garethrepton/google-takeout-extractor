"""Cache module for persisting extracted data to SQLite."""
import sqlite3
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple
from rich.console import Console

console = Console(legacy_windows=(sys.platform == "win32"))

# Default cache location
DEFAULT_CACHE_DIR = Path.home() / ".googletakeout"
DEFAULT_CACHE_FILE = DEFAULT_CACHE_DIR / "takeout_cache.db"


class TakeoutCache:
    """Thread-safe SQLite cache for storing extracted file metadata."""

    def __init__(self, cache_path: Optional[Path] = None):
        self.cache_path = cache_path or DEFAULT_CACHE_FILE
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()
        self._pending_dates: list = []  # Buffer for batch inserts
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        self.conn = sqlite3.connect(str(self.cache_path), check_same_thread=False)

        # Table for EXIF dates
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_dates (
                zip_path TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_crc INTEGER NOT NULL,
                extracted_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (zip_path, file_path)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dates_zip ON file_dates(zip_path)
        """)

        # Table for directory file hashes (for comparison against output directory)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS directory_files (
                dir_path TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                file_mtime REAL NOT NULL,
                content_key TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (dir_path, file_path)
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dir_files ON directory_files(dir_path)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_dir_content_key ON directory_files(content_key)
        """)

        # Table for directory scan metadata
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS directory_scans (
                dir_path TEXT PRIMARY KEY,
                last_scanned TIMESTAMP NOT NULL,
                file_count INTEGER NOT NULL
            )
        """)

        self.conn.commit()

    def get_date(self, zip_path: Path, file_path: str, file_size: int, file_crc: int) -> Optional[datetime]:
        """
        Get a cached date for a file if it exists and CRC matches.

        Returns:
            The cached datetime if found and valid, None otherwise
        """
        if not self.conn:
            return None

        with self._lock:
            cursor = self.conn.execute(
                """
                SELECT extracted_date FROM file_dates
                WHERE zip_path = ? AND file_path = ? AND file_size = ? AND file_crc = ?
                """,
                (str(zip_path), file_path, file_size, file_crc)
            )
            row = cursor.fetchone()
            if row and row[0]:
                try:
                    return datetime.fromisoformat(row[0])
                except ValueError:
                    return None
            return None

    def set_date(self, zip_path: Path, file_path: str, file_size: int, file_crc: int, date: Optional[datetime]) -> None:
        """Store a date in the cache. Thread-safe, batched for performance."""
        if not self.conn:
            return

        date_str = date.isoformat() if date else None

        with self._lock:
            self._pending_dates.append((str(zip_path), file_path, file_size, file_crc, date_str))

            # Batch commit every 100 entries for performance
            if len(self._pending_dates) >= 100:
                self._flush_dates()

    def _flush_dates(self) -> None:
        """Flush pending date inserts to database. Must be called with lock held."""
        if not self._pending_dates:
            return

        self.conn.executemany(
            """
            INSERT OR REPLACE INTO file_dates (zip_path, file_path, file_size, file_crc, extracted_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            self._pending_dates
        )
        self.conn.commit()
        self._pending_dates.clear()

    def flush(self) -> None:
        """Flush any pending writes to disk."""
        with self._lock:
            self._flush_dates()

    def get_cached_dates_bulk(self) -> Dict[Tuple[str, str, int, int], datetime]:
        """
        Load all cached dates into memory for fast lookup.

        Returns:
            Dictionary mapping (zip_path, file_path, size, crc) -> datetime
        """
        if not self.conn:
            return {}

        cursor = self.conn.execute(
            "SELECT zip_path, file_path, file_size, file_crc, extracted_date FROM file_dates"
        )

        result = {}
        for row in cursor.fetchall():
            if row[4]:  # Has date
                try:
                    result[(row[0], row[1], row[2], row[3])] = datetime.fromisoformat(row[4])
                except ValueError:
                    pass
        return result

    def get_cached_count(self) -> int:
        """Get the number of cached date entries."""
        if not self.conn:
            return 0

        cursor = self.conn.execute("SELECT COUNT(*) FROM file_dates")
        return cursor.fetchone()[0]

    # === Directory file caching methods ===

    def get_directory_files(self, dir_path: Path) -> Dict[str, Tuple[int, float, str]]:
        """
        Get cached file info for a directory.

        Returns:
            Dict mapping relative file path -> (size, mtime, content_key)
        """
        if not self.conn:
            return {}

        with self._lock:
            cursor = self.conn.execute(
                """
                SELECT file_path, file_size, file_mtime, content_key
                FROM directory_files WHERE dir_path = ?
                """,
                (str(dir_path),)
            )
            return {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}

    def get_directory_content_keys(self, dir_path: Path) -> Dict[int, set]:
        """
        Get cached content keys indexed by file size for fast lookup.

        Returns:
            Dict mapping file_size -> set of content_keys
        """
        if not self.conn:
            return {}

        with self._lock:
            cursor = self.conn.execute(
                """
                SELECT file_size, content_key FROM directory_files WHERE dir_path = ?
                """,
                (str(dir_path),)
            )
            result: Dict[int, set] = {}
            for size, key in cursor.fetchall():
                if size not in result:
                    result[size] = set()
                result[size].add(key)
            return result

    def set_directory_file(self, dir_path: Path, file_path: str, file_size: int,
                           file_mtime: float, content_key: str) -> None:
        """Store a directory file entry in the cache."""
        if not self.conn:
            return

        with self._lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO directory_files
                (dir_path, file_path, file_size, file_mtime, content_key)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(dir_path), file_path, file_size, file_mtime, content_key)
            )

    def set_directory_files_bulk(self, dir_path: Path,
                                  files: list) -> None:
        """
        Bulk insert directory file entries.

        Args:
            dir_path: The directory path
            files: List of (file_path, file_size, file_mtime, content_key) tuples
        """
        if not self.conn or not files:
            return

        with self._lock:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO directory_files
                (dir_path, file_path, file_size, file_mtime, content_key)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(str(dir_path), fp, fs, fm, ck) for fp, fs, fm, ck in files]
            )
            self.conn.commit()

    def remove_directory_file(self, dir_path: Path, file_path: str) -> None:
        """Remove a single file from directory cache (e.g., if deleted)."""
        if not self.conn:
            return

        with self._lock:
            self.conn.execute(
                "DELETE FROM directory_files WHERE dir_path = ? AND file_path = ?",
                (str(dir_path), file_path)
            )
            self.conn.commit()

    def clear_directory(self, dir_path: Path) -> None:
        """Clear all cached entries for a specific directory."""
        if not self.conn:
            return

        with self._lock:
            self.conn.execute(
                "DELETE FROM directory_files WHERE dir_path = ?",
                (str(dir_path),)
            )
            self.conn.commit()

    def get_directory_cache_count(self, dir_path: Path) -> int:
        """Get the number of cached files for a directory."""
        if not self.conn:
            return 0

        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM directory_files WHERE dir_path = ?",
            (str(dir_path),)
        )
        return cursor.fetchone()[0]

    def get_directory_last_scan(self, dir_path: Path) -> Optional[Tuple[datetime, int]]:
        """
        Get the last scan timestamp and file count for a directory.

        Returns:
            Tuple of (last_scanned datetime, file_count) or None if not cached
        """
        if not self.conn:
            return None

        cursor = self.conn.execute(
            "SELECT last_scanned, file_count FROM directory_scans WHERE dir_path = ?",
            (str(dir_path),)
        )
        row = cursor.fetchone()
        if row:
            try:
                return (datetime.fromisoformat(row[0]), row[1])
            except ValueError:
                return None
        return None

    def set_directory_last_scan(self, dir_path: Path, file_count: int) -> None:
        """Update the last scan timestamp for a directory."""
        if not self.conn:
            return

        with self._lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO directory_scans (dir_path, last_scanned, file_count)
                VALUES (?, ?, ?)
                """,
                (str(dir_path), datetime.now().isoformat(), file_count)
            )
            self.conn.commit()

    def clear(self) -> None:
        """Clear all cached entries."""
        if not self.conn:
            return

        with self._lock:
            self._pending_dates.clear()
            self.conn.execute("DELETE FROM file_dates")
            self.conn.execute("DELETE FROM directory_files")
            self.conn.commit()

    def close(self) -> None:
        """Close the database connection, flushing pending writes."""
        if self.conn:
            self.flush()
            self.conn.close()
            self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
