from logging.handlers import TimedRotatingFileHandler
import os
import gzip


class GzipSizedTimedRotatingFileHandler(TimedRotatingFileHandler):
    """A file handler rotates collector gzip file based on size and time thresholds.
     Writes directly to balanced gzip compressed files on the fly.
    Inherits from TimedRotatingFileHandler and adds size-based rotation capability.
    Rotation occurs when EITHER the size limit or time interval is exceeded.

    Args:
        filename (str): Path to the log file.
        max_bytes (int): Maximum file size in bytes before rotation (0 = no size limit).
        backupCount (int): Number of backup files to retain.
        when (str): Time rotation interval type ('S', 'M', 'H', 'D', etc.).
        interval (int): Time interval between rotations.
        compresslevel (int): Gzip compression level (1-9).

    Example:
        >>> handler = GzipSizedTimedRotatingFileHandler(
        ...     'app.log',
        ...     maxBytes=10*1024*1024,  # 10 MB
        ...     backupCount=5,
        ...     when='M',               # Minutes
        ...     interval=30             # Every 30 mins
        ... )
    """

    def __init__(
        self,
        filename,
        maxBytes=1024 * 1024,
        backupCount=1,
        when="M",
        interval=1,
        compresslevel=6,
        **kwargs,
    ):
        filename = filename if filename.endswith(".gz") else f"{filename}.gz"
        super().__init__(
            filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding="utf-8",
            delay=True,
        )
        self.max_bytes = maxBytes
        self.current_size = 0
        self.compresslevel = compresslevel
        self._check_interval = 100 * 1024  # 100kb
        self.rotation_callback = kwargs.pop("rotation_callback", None)
        self._open()

    def shouldRollover(self, record):
        """Determine if rollover should occur.

        Args:
            record (LogRecord): The log record being emitted.

        Returns:
            bool: True if rollover should occur, False otherwise.
        """
        # Size-based check
        if self.max_bytes > 0 and self.current_size >= self._check_interval:
            if os.path.getsize(self.baseFilename) >= self.max_bytes:
                return True
            self.current_size = 0
        # Time-based check
        return super().shouldRollover(record)

    def _open(self):
        """Open the current log file with gzip compression."""
        if self.stream:
            self.stream.close()
        self.stream: gzip.GzipFile = gzip.open(
            self.baseFilename, mode="ab", compresslevel=self.compresslevel
        )
        self.current_size = os.path.getsize(self.baseFilename)

    def emit(self, record):
        """Write the log record to compressed file"""
        try:
            msg = self.format(record) + self.terminator
            compressed = msg.encode(self.encoding)
            self.stream.write(compressed)
            self.stream.flush()
            self.current_size += len(compressed)
            if self.shouldRollover(record):
                self.doRollover()
        except Exception:
            self.handleError(record)

    def doRollover(self):
        if self.stream:
            self.stream.close()

        # use parent handler for rollover
        super().doRollover()

        # Open new compressed file
        self._open()

        if self.rotation_callback:
            self.rotation_callback()

    def close(self):
        """Close the handler and ensure all data is flushed."""
        if self.stream:
            self.stream.flush()
            self.stream.close()
        # Still call parent cleanup for other resources
        super().close()
