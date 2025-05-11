from enum import StrEnum


class MetaDataKey(StrEnum):
    COLLECTED = "collected"
    FLUSHED = "flushed"
    ERRORED = "errored"
