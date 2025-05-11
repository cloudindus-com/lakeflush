import time
from lakeflush.utils.s3.store import S3Store


class S3CSVFileReader:
    """Reads csv file and processes the content further"""

    def __init__(self, header: bool, bucket: str, batch_size=100) -> None:
        self.header = header
        self.header_data = None
        self.batch_size = batch_size
        self.bucket = bucket

    def read(self, object_key: str):
        res = S3Store.get(self.bucket, object_key)
        with res["Body"] as stream:
            lines = stream.iter_lines()
            if self.header:
                if not self.header_data:
                    # Store header
                    self.header_data = next(lines).strip()
                    yield self.header_data
                else:
                    # Skip header
                    next(lines)

            # Process in batch_size
            batch = []
            for line in lines:
                batch.append(line)
                if len(batch) >= self.batch_size:
                    yield "".join(batch).strip()
                    batch = []
                    # relax CPU
                    time.sleep(0.001)

            if batch:
                yield "".join(batch).strip()
