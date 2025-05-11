from lakeflush.utils.s3.store import S3Store


class S3JSONFileReader:
    """Reads json fle and processes the content further"""

    def __init__(self, bucket: str) -> None:
        # added for common check
        self.header_data = None
        self.bucket = bucket

    def read(self, object_key: str):
        res = S3Store.get(self.bucket, object_key)
        if "Body" in res:
            yield res["Body"].read()
