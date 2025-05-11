from __future__ import annotations
from typing import TYPE_CHECKING


__all__ = ["LocalLakeCollector", "S3LakeCollector"]

__COLLECTORS__ = {
    "LocalLakeCollector": "local_lake",
    "S3LakeCollector": "s3_lake",
}


def __getattr__(name: str):
    if name in __COLLECTORS__:
        module_name = __COLLECTORS__[name]
        import importlib

        module = importlib.import_module(f"lakeflush.collectors.{module_name}", name)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


if TYPE_CHECKING:
    from lakeflush.collectors.local_lake import LocalLakeCollector
    from lakeflush.collectors.s3_lake import S3LakeCollector
