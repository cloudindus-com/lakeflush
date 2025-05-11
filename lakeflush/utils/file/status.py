from enum import StrEnum


class FileStatus(StrEnum):
    INPROGRESS = ".inprogress"
    COLLECTED = ".collected"
    FLUSHED = ".flushed"
