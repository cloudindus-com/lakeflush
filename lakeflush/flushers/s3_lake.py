from datetime import datetime
from botocore.exceptions import ClientError

from lakeflush.core import Flusher
from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileStore, FileStatus
from lakeflush.utils.s3 import S3Store


class S3LakeFlusher(Flusher):
    """Flushes collected large files to the s3 bucket path or datalake partition.
    Existing file will be overwritten in the s3 path or datalake partition.

    Args:
        bucket (str): The destination s3 bucket or datalake to flush file.
        filepath (str): The same file path provided for collector.
        filename (str): The same file name provided for collector.
        prefix (str): The path or dir in s3 bucket to flush object (default root).
        date_partition_format Optional(str): If provided creates partiton pattern based
            on current datetime format before flusing file. eg: year=%Y/month=%m/day=%d

    Example:
        >>> s3_flusher = S3LakeFlusher(bucket, filepath, filename)
        >>> s3_flusher.start()

    """

    def __init__(
        self,
        bucket: str,
        filepath: str,
        filename: str,
        date_partition_format: str = None,
    ):

        super().__init__(filepath, filename)

        if not bucket:
            raise ValueError("bucket is required.")

        S3Store.setup()

        self.bucket = bucket

        if not S3Store.exists(self.bucket):
            raise ValueError(f"S3 bucket does not exist: {bucket}")

        Logger.info("setup s3-flusher")

        self.partition_format = date_partition_format

    def flush(self, src_file: str):
        """flush collected file to s3"""
        try:
            basename = FileStore.basename(src_file)
            object_key = basename.replace(FileStatus.COLLECTED, "")
            flush_path = ""
            if self.partition_format:
                # create partition based on format provided
                flush_path = datetime.now().strftime(self.partition_format) + "/"
            # flush object to s3 flush path
            S3Store.upload(src_file, self.bucket, f"{flush_path}{object_key}")
            Logger.info(f"flushed object {object_key} to s3 path: {flush_path}")
            # write meta data
            metaname = basename.replace(FileStatus.COLLECTED, FileStatus.FLUSHED)
            FileStore.flushmeta(metaname, flush_path)
        except ClientError as ex:
            Logger.error(f"s3_client error: {ex}")
        except Exception as e:
            Logger.error(f"unexpected error flushing file to s3: {str(e)}")
