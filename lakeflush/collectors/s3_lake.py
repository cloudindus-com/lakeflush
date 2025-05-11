from lakeflush.core import Collector
from typing import List
from botocore.exceptions import ClientError

from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileType
from lakeflush.utils.s3 import S3Processor, S3Store
from lakeflush.utils.s3.reader import S3CSVFileReader, S3JSONFileReader


class S3LakeCollector(Collector):
    """An aws S3 lake collector collects all objects in s3 data lake recursively
    into large files efficiently in sequence using file last modified time.

    Args:
        bucket (str): The aws s3 bucket name to process files from.
        prefix (str): The s3 path in bucket to the objects directory (default root).
        file_type (FileType): The type of file. Either 'json' or 'csv' (default 'json').
        match_patterns (List[str]): The list of patterns to match files in s3 data lake,
            uses unix pattern style for eg: ["*.json"].
        batch_size (int): The size of batch to process files at once.
        csv_header (bool): For file_type csv, If True extracts header from first
            file and put in all collected, otherwise skips header. (default = False)
        log_file (bool): If True logs the name of file (default = False).
        **kwargs: The parent class arguments. See Collector.

    Example:
        >>> s3_collector = S3LakeCollector(bucket, FileType, filepath, filename)
        >>> s3_collector.start()
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = None,
        s3_batchsize: int = 1000,
        file_type: FileType = FileType.JSON,
        match_patterns: List[str] = [],
        batch_size: int = 1000,
        csv_header: bool = False,
        log_file: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        Logger.info("setup s3-collector")

        if not bucket:
            raise ValueError("s3 bucket name is required.")

        S3Store.setup()

        if not S3Store.exists(bucket):
            raise ValueError(f"S3 bucket does not exist: {bucket}")

        self.processor = S3Processor(
            bucket,
            prefix,
            s3_batchsize,
            match_patterns,
            batch_size,
        )

        if file_type == FileType.CSV:
            self.reader = S3CSVFileReader(csv_header, bucket)
        else:
            self.reader = S3JSONFileReader(bucket)
        self.log_file = log_file

    def process_files_by_mtime(self):
        """Find matched s3 objects keys, sorted by modification time."""
        for object_key in iter(self.processor):
            if self.log_file:
                Logger.info(f"processing s3 object: {object_key}")
            try:
                # read data from s3 object reader
                for data in self.reader.read(object_key):
                    self.collect(data)
            except ClientError as ex:
                Logger.error(f"s3_client error: {ex}")
            except Exception as ex:
                Logger.error(f"unexpected error: {str(ex)}")

    def on_collected(self):
        """Callback after collection"""
        if self.reader.header_data:
            self.collect(self.reader.header_data)

    def start(self):
        """Starts collector and processes files from s3"""
        Logger.info("starting s3-collector")
        self.process_files_by_mtime()
