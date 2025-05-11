import os
import heapq
import fnmatch
from pathlib import Path
from typing import Iterator, List
from lakeflush.utils.logger import Logger


class FileProcessor:
    """A memory-efficient streaming processor for files sorted by modification time.

    This class implements an iterator that yields files from a directory tree in
    ascending order of their modification times by batch (oldest first) without
    loading all file paths into memory simultaneously. Supports file pattern matching.
    """

    def __init__(
        self,
        root_dir: str | Path,
        match_patterns: List[str] = [],
        batch_size: int = 1000,
    ):
        """Initialize the file processor.

        Args:
            root_dir (str or Path object): Path to the root directory
            match_patterns (List of string): patterns to match file names (default all)
            batch_size (int): Batch size to control number of files (default 1000)
        """
        self.root = Path(root_dir)
        self.batch_size = batch_size
        self.match_patterns = tuple(match_patterns)
        self._heap = []
        self._dir_queue = []
        self._current_dir = None
        self._dir_iter = None

    def _should_match(self, path: Path) -> bool:
        """Check if file matches inclusion criteria."""
        if not self.match_patterns:
            return True

        filename = path.name
        # Check patterns
        pattern_match = any(
            fnmatch.fnmatch(filename, pattern) for pattern in self.match_patterns
        )
        if not pattern_match:
            return False
        return True

    def __iter__(self) -> Iterator[Path]:
        """Initialize the iterator.

        Returns:
            An iterator yielding Path objects in mtime order
        """
        self._dir_queue = [self.root]
        return self

    def __next__(self) -> Path:
        """Get the next file in modification time order.

        Returns:
            Path object for the next file

        Raises:
            StopIteration: When no more files remain to process
        """
        while True:
            # Try to get next file from heap
            if self._heap:
                mtime, path = heapq.heappop(self._heap)
                return path

            # Need to scan more directories
            if not self._load_next_batch():
                raise StopIteration

    def _load_next_batch(self) -> bool:
        """Scan directories to populate the processing heap.

        Scans directories incrementally until either:
        - The heap contains files to process
        - All directories have been exhausted

        Returns:
            bool: True if files are available in heap, False if processing complete
        """
        while self._dir_queue or self._current_dir:
            if self._current_dir is None:
                self._current_dir = self._dir_queue.pop()
                try:
                    self._dir_iter = os.scandir(self._current_dir)
                except (PermissionError, OSError):
                    self._current_dir = None
                    continue

            try:
                entry = next(self._dir_iter)
                try:
                    if entry.is_dir(follow_symlinks=False):
                        self._dir_queue.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        filepath = Path(entry.path)
                        if self._should_match(filepath):
                            stat = entry.stat()
                            heapq.heappush(self._heap, (stat.st_mtime, filepath))
                            # Control memory usage using batch
                            if len(self._heap) > self.batch_size:
                                return True
                except (OSError, PermissionError) as ex:
                    Logger.warning(f"OSError: {ex}")
                    continue
            except StopIteration:
                self._current_dir = None

        return len(self._heap) > 0
