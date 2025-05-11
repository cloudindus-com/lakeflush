from lakeflush.core import Collector
from typing import List

from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileProcessor, FileType, FileStore
from lakeflush.utils.file.reader import CSVFileReader, JSONFileReader


class LocalLakeCollector(Collector):
    """A local lake collector collects all files in local data lake or directories
    recursively into large files efficiently in sequence using file last modified time.

    Args:
        root_dir (str): The path of root_dir to process files from.
        file_type (FileType): The type of file. Either 'json' or 'csv' (default 'json').
        match_patterns (List[str]): The list of patterns to match files in directory
            or lake, uses unix pattern style for eg: ["*.json"].
        batch_size (int): The size of batch to process files at once.
        csv_header (bool): For file_type csv, If True extracts header from first
            file and put in all collected, otherwise skips header. (default = False)
        log_file (bool): If True logs the name of file (default = False).
        **kwargs: The parent class arguments. See Collector.

    Example:
        >>> local_collector = LocalLakeCollector(root_dir, FileType, filepath, filename)
        >>> local_collector.start()
    """

    def __init__(
        self,
        root_dir: str,
        file_type: FileType = FileType.JSON,
        match_patterns: List[str] = [],
        batch_size: int = 1000,
        csv_header: bool = False,
        log_file: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        Logger.info("setup local-collector")

        if not root_dir:
            raise ValueError("root_dir is required.")

        self.processor = FileProcessor(root_dir, match_patterns, batch_size)

        if not self.processor.root.exists():
            raise ValueError(f"Directory does not exist: {root_dir}")

        if not self.processor.root.is_dir():
            raise ValueError(f"Path is not a directory: {root_dir}")

        if file_type == FileType.CSV:
            self.reader = CSVFileReader(csv_header)
        else:
            self.reader = JSONFileReader()
        self.log_file = log_file

    def process_files_by_mtime(self):
        """Find matched files path, sorted by modification time."""
        for file_path in iter(self.processor):
            if self.log_file:
                Logger.info(f"processing file: {FileStore.basename(file_path)}")
            try:
                # read data from file reader
                for data in self.reader.read(file_path):
                    self.collect(data)
            except (OSError, PermissionError):
                Logger.warning(f"permission error while reading file: {file_path}")
            except Exception as ex:
                Logger.error(f"unexpected error: {str(ex)}")

    def on_collected(self):
        """Callback after collection"""
        if self.reader.header_data:
            self.collect(self.reader.header_data)

    def start(self):
        """Starts collector and processes files"""
        Logger.info("starting local-collector")
        self.process_files_by_mtime()
