from __future__ import annotations
from typing import TYPE_CHECKING

__all__ = ["LocalLakeFlusher", "S3LakeFlusher"]

__FLUSHERS__ = {
    "LocalLakeFlusher": "local_lake",
    "S3LakeFlusher": "s3_lake",
}


def __getattr__(name: str):
    if name in __FLUSHERS__:
        module_name = __FLUSHERS__[name]
        import importlib

        module = importlib.import_module(f"lakeflush.flushers.{module_name}", name)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__


if TYPE_CHECKING:
    from lakeflush.flushers.local_lake import LocalLakeFlusher
    from lakeflush.flushers.s3_lake import S3LakeFlusher
