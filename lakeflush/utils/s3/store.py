from pathlib import Path
import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError


class S3Store:
    """S3 Store util interacts with s3 client APIs"""

    __client__: BaseClient

    @classmethod
    def setup(cls):
        """Setups s3 client"""
        cls.__client__ = boto3.client("s3")

    @classmethod
    def paginator(cls):
        """Returns s3 list paginator from client"""
        return cls.__client__.get_paginator("list_objects_v2")

    @classmethod
    def exists(cls, bucket: str) -> bool:
        """
        Determine whether the s3 bucket exists.

        :return bool: True when the bucket exists; otherwise, False.
        """
        try:
            cls.__client__.head_bucket(Bucket=bucket)
            return True
        except ClientError:
            return False

    @classmethod
    def get(cls, bucket: str, key: str) -> dict:
        """
        Get the object stored in s3 bucket.

        :return dict: s3 reposne object.
        """
        return cls.__client__.get_object(Bucket=bucket, Key=key)

    @classmethod
    def upload(cls, file_path: Path, bucket: str, key: str):
        """Uploads the file to s3 bucket."""
        return cls.__client__.upload_file(Filename=file_path, Bucket=bucket, Key=key)
