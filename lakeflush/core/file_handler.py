from logging.handlers import TimedRotatingFileHandler
import os


class SizedTimedRotatingFileHandler(TimedRotatingFileHandler):
    """A file handler rotates collector file based on both size and time thresholds.

    Inherits from TimedRotatingFileHandler and adds size-based rotation capability.
    Rotation occurs when EITHER the size limit or time interval is exceeded.

    Args:
        filename (str): Path to the log file.
        max_bytes (int): Maximum file size in bytes before rotation (0 = no size limit).
        backupCount (int): Number of backup files to retain.
        when (str): Time rotation interval type ('S', 'M', 'H', 'D', etc.).
        interval (int): Time interval between rotations.

    Example:
        >>> handler = SizedTimedRotatingFileHandler(
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
        **kwargs,
    ):
        super().__init__(
            filename, when=when, interval=interval, backupCount=backupCount
        )
        self.max_bytes = maxBytes
        self.rotation_callback = kwargs.pop("rotation_callback", None)

    def shouldRollover(self, record):
        """Determine if rollover should occur.

        Args:
            record (LogRecord): The log record being emitted.

        Returns:
            bool: True if rollover should occur, False otherwise.
        """
        # Size-based check
        if self.max_bytes > 0:
            msg = f"{self.format(record)}\n"
            self.stream.seek(0, os.SEEK_END)
            if self.stream.tell() + len(msg) >= self.max_bytes:
                return True
        # Time-based check
        return super().shouldRollover(record)

    def doRollover(self):
        # use parent handler for rollover
        super().doRollover()

        if self.rotation_callback:
            self.rotation_callback()
