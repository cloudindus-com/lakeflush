from lakeflush.utils.metadata.key import MetaDataKey
from typing import Any


class MetaDataStore:
    """Stores application meta data and global variables"""

    __metadata: dict = {}

    @classmethod
    def setup(cls):
        """Configures application metadata"""
        cls.__metadata[MetaDataKey.COLLECTED] = 0
        cls.__metadata[MetaDataKey.FLUSHED] = 0
        cls.__metadata[MetaDataKey.ERRORED] = 0

    @classmethod
    def set(cls, key: MetaDataKey, value: Any) -> None:
        cls.__metadata[key] = value

    @classmethod
    def get(cls, key: MetaDataKey) -> Any:
        return cls.__metadata.get(key)
