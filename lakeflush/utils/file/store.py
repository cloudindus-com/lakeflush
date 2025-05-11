import os
from pathlib import Path


class FileStore:
    """File Store util interacts with os module"""

    __lakeflush_path: Path

    @classmethod
    def setup(cls, path="."):
        """Configures application meta dirs"""
        cls.__lakeflush_path = Path(f"{os.path.realpath(path)}/.lakeflush")
        os.makedirs(cls.__lakeflush_path, mode=0o700, exist_ok=True)

    @classmethod
    def flushmeta(cls, meta_filename: Path | str, dest_filepath: Path | str):
        """Configures application meta dirs"""
        meta_filepath = cls.__lakeflush_path / meta_filename
        with open(meta_filepath, "w") as fp:
            fp.write(str(dest_filepath))

    @classmethod
    def format(cls, path: str, name: str, status: str) -> str:
        """Creates lakeflush filename format from path and name"""
        return f"{os.path.realpath(path)}/{name}.lakeflush{status}"

    @classmethod
    def basename(cls, filepath: str) -> str:
        """Returns file basename from a file path"""
        return os.path.basename(filepath)

    @classmethod
    def exists(cls, path: str) -> bool:
        """Checks if path exists"""
        return os.path.exists(path)

    @classmethod
    def mkdirs(cls, path: str) -> None:
        "Create multiple directory in path"
        os.makedirs(path, exist_ok=True)

    @classmethod
    def empty(cls, path: str) -> bool:
        """Checks if file is empty"""
        return os.path.getsize(path) == 0
