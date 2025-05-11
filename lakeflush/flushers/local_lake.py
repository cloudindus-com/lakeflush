import shutil
from pathlib import Path
from datetime import datetime

from lakeflush.core import Flusher
from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileStore, FileStatus


class LocalLakeFlusher(Flusher):
    """Flushes collected large files to the local path or datalake partition.
    Existing file will be overwritten in the destination path or datalake partition.

    Args:
        root_dir (str): The destination path or datalake to flush file.
        filepath (str): The same file path provided for collector.
        filename (str): The same file name provided for collector.
        date_partition_format Optional(str): If provided creates partiton pattern based
            on current datetime format before flusing file. eg: year=%Y/month=%m/day=%d

    Example:
        >>> local_flusher = LocalLakeFlusher(root_dir, filepath, filename)
        >>> local_flusher.start()

    """

    def __init__(
        self,
        root_dir: str,
        filepath: str,
        filename: str,
        date_partition_format: str = None,
    ):

        super().__init__(filepath, filename)

        if not root_dir:
            raise ValueError("root_dir is required.")

        self.root = Path(root_dir)

        if not FileStore.exists(self.root):
            raise ValueError("root_dir provided does not exist.")

        Logger.info("setup local-flusher")

        self.partition_format = date_partition_format

    def flush(self, src_file: str):
        """flush collected file"""
        try:
            basename = FileStore.basename(src_file)
            destname = basename.replace(FileStatus.COLLECTED, "")
            flush_path = self.root / destname
            if self.partition_format:
                # create partition based on format provided
                partition_path = datetime.now().strftime(self.partition_format)
                flush_path = self.root / partition_path
                FileStore.mkdirs(flush_path)
                flush_path = flush_path / destname
            # flush file to flush path
            shutil.move(src_file, flush_path)
            file_path = str(flush_path).replace(str(self.root), "")
            Logger.info(f"flushed file {destname} to path: {file_path}")
            # write meta data
            metaname = basename.replace(FileStatus.COLLECTED, FileStatus.FLUSHED)
            FileStore.flushmeta(metaname, flush_path)
        except Exception as e:
            Logger.error(f"error flushing file: {str(e)}")
