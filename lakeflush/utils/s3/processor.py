import heapq
import fnmatch
from typing import Iterator, List
from botocore.exceptions import ClientError

from lakeflush.utils.s3.store import S3Store
from lakeflush.utils.logger import Logger


class S3Processor:
    """A memory-efficient streaming s3 processor to list objects sorted in s3 bucket.

    This class implements an iterator that yields objects from a s3 bucket in
    ascending order of their modification times by batch (oldest first) without
    loading all objects into memory simultaneously. Supports file pattern matching.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = None,
        s3_batchsize: int = 1000,
        match_patterns: List[str] = [],
        batch_size: int = 1000,
    ):
        """Initialize the file processor.

        Args:
            bucket (str): The s3 bucket to read and process objects from
            prefix (str): The s3 path in bucket to the root directory (default None)
            s3_batchsize (int): Batch size to paginate s3 objects (default 1000)
            match_patterns (List of string): patterns to match object names(default all)
            batch_size (int): Batch size to control number of files (default 1000)
        """
        self.paginator = S3Store.paginator()
        self.pg_params = dict(
            Bucket=bucket, PaginationConfig={"PageSize": s3_batchsize}
        )
        if prefix:
            self.pg_params["Prefix"] = prefix
        self.batch_size = batch_size
        self.match_patterns = tuple(match_patterns)
        self._heap = []

    def _should_match(self, object_key: str) -> bool:
        """Check if s3 object key matches inclusion criteria."""
        if not self.match_patterns:
            return True

        # Check patterns
        pattern_match = any(
            fnmatch.fnmatch(object_key, pattern) for pattern in self.match_patterns
        )
        if not pattern_match:
            return False
        return True

    def __iter__(self) -> Iterator:
        """Initialize the iterator"""
        return self

    def __next__(self) -> str:
        """Get the next object in modification time order.

        Returns:
            str: s3 object key for the next file

        Raises:
            StopIteration: When no more s3 object remain to process
        """
        while True:
            # Try to get next object key from heap
            if self._heap:
                mtime, object_key = heapq.heappop(self._heap)
                return object_key

            # Need to scan more path
            if not self._load_next_batch():
                raise StopIteration

    def _load_next_batch(self) -> bool:
        """Scan s3 directories to populate the processing heap.

        Scans s3 directories incrementally until either:
        - The heap contains files to process
        - All objects have been exhausted

        Returns:
            bool: True if files are available in heap, False if processing complete
        """
        try:
            for page in self.paginator.paginate(**self.pg_params):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith("/"):
                        continue
                    if not self._should_match(obj["Key"]):
                        continue

                    heapq.heappush(self._heap, (obj["LastModified"], obj["Key"]))
                    # Control memory usage using batch
                    if len(self._heap) > self.batch_size:
                        return True
        except ClientError as ex:
            Logger.error(f"s3_client error: {ex}")
        except StopIteration:
            Logger.info("all s3 objects listed")
        except Exception as ex:
            Logger.error(f"unexpected error: {str(ex)}")

        return len(self._heap) > 0
