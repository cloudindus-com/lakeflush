import logging
import uuid
import time
from lakeflush.core.file_handler import SizedTimedRotatingFileHandler
from lakeflush.core.gzipfile_handler import GzipSizedTimedRotatingFileHandler
from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileStore, FileStatus
from lakeflush.utils.metadata import MetaDataStore


class Collector:
    """Collects data into a file, rotates file based on time and size defined.

    Args:
        filepath (str): Path to the file.
        filename (str): Name to the file.
        max_size_mb (int): Maximum file size in MB before rotation, default (1 MB).
        max_time_mins (int): Maximum time in min before rotation, default (1 min).
        compress (bool): Compresses file to gzip, default (10000).

    Example:
        >>> collector = Collector(filepath, filename)
        >>> collector.start()
        >>> collector.collect(data)
    """

    def __init__(
        self,
        filepath: str,
        filename: str,
        max_size_mb: int = 1,
        max_time_mins: int = 1,
        compress: bool = False,
    ):
        if not filepath or not filename:
            raise ValueError("filepath and filename is required.")

        if not FileStore.exists(filepath):
            raise ValueError("filepath provided does not exists.")

        if max_size_mb < 1:
            raise ValueError("max_size_mb cannot be less than 1.")

        if max_time_mins < 1:
            raise ValueError("max_time_mins cannot be less than 1.")

        # Setup
        Logger.setup()
        FileStore.setup()
        MetaDataStore.setup()
        Logger.info("setup collector")

        self.path = filepath
        self.name = filename
        self.compress = compress

        if self.compress:
            file_handler = GzipSizedTimedRotatingFileHandler(
                FileStore.format(self.path, self.name, FileStatus.INPROGRESS),
                maxBytes=max_size_mb * 1024 * 1024,
                backupCount=10,
                when="M",
                interval=max_time_mins,
                rotation_callback=self.on_collected,
            )
        else:
            file_handler = SizedTimedRotatingFileHandler(
                FileStore.format(self.path, self.name, FileStatus.INPROGRESS),
                maxBytes=max_size_mb * 1024 * 1024,
                backupCount=10,
                when="M",
                interval=max_time_mins,
                rotation_callback=self.on_collected,
            )
        file_handler.namer = self.lakeflush_namer
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        file_handler.setLevel(logging.INFO)
        self.collector = logging.getLogger("__lakeflush-collector__")
        self.collector.setLevel(logging.INFO)
        self.collector.addHandler(file_handler)

    def lakeflush_namer(self, default_name: str) -> str:
        """Converts '<filename>' to '<filename>.<timestamp>.lakeflush.collected.'"""
        base_name = (
            f"{self.name}.{int(time.time())}.{str(uuid.uuid4()).replace('-','')}"
        )
        file_path = FileStore.format(self.path, base_name, FileStatus.COLLECTED)
        if self.compress:
            file_path = f"{file_path}.gz"
        Logger.info(f"collected file {FileStore.basename(file_path)}")
        return file_path

    def on_collected(self) -> None:
        """Callback after file collection and new file creation"""
        pass

    def collect(self, data: str) -> None:
        """Collects data into a file '<filename>.lakeflush.inprogress'"""
        try:
            self.collector.info(data)
        except Exception as ex:
            Logger.error(str(ex))
            raise ex
