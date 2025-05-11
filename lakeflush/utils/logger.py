import logging
import sys


class Logger:

    __logger: logging.Logger

    @classmethod
    def setup(cls, name="lakeflush", level=logging.INFO):
        """Configures application logger"""
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Prevent duplicate handlers if called multiple times
        if logger.hasHandlers():
            logger.handlers.clear()

        # Formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console Handler (stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

        cls.__logger = logger
        cls.info("setup logger")

    @classmethod
    def info(cls, msg: str):
        return cls.__logger.info(msg)

    @classmethod
    def error(cls, msg: str):
        return cls.__logger.error(msg)

    @classmethod
    def warning(cls, msg: str):
        return cls.__logger.warning(msg)
