from watchdog.observers import Observer
from lakeflush.core.event_handler import FileRotationEventHandler
from lakeflush.utils.logger import Logger
from lakeflush.utils.file import FileStore, FileStatus
import time


class Flusher:
    """Flushes collected files to their destination in real time.

    Args:
        filepath (str): Path to the file.
        filename (str): Name to the file.

    Example:
        >>> flusher = Flusher(filepath, filename)
        >>> flusher.start()

    """

    def __init__(self, filepath: str, filename: str):

        if not filepath or not filename:
            raise ValueError("filepath and filename is required.")

        if not FileStore.exists(filepath):
            raise ValueError("filepath provided does not exists.")

        # Setup
        Logger.setup()
        FileStore.setup()
        Logger.info("setup flusher")
        self.path = filepath
        self.name = filename
        self.keyword = ".lakeflush" + FileStatus.COLLECTED

    def on_collected(self, dest_path: bytes | str):
        dest_path = str(dest_path)
        Logger.info(f"detected new file {FileStore.basename(dest_path)}")
        if not FileStore.empty(dest_path):
            return self.flush(dest_path)
        Logger.info(f"skipping flush empty file {FileStore.basename(dest_path)}")

    def flush(self, collected_filepath: str):
        """flush collected file"""
        raise NotImplementedError

    def start(self):
        """starts the flusher"""
        Logger.info("starting flusher")
        handler = FileRotationEventHandler(self.keyword)
        handler.on_collected = self.on_collected
        observer = Observer()
        observer.schedule(handler, self.path)
        self._running = True
        try:
            observer.start()
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            Logger.warning("keyboard interruption")
        except Exception as ex:
            Logger.error(f"unexpected error: {str(ex)}")
        finally:
            Logger.info("stopping flusher")
            observer.stop()
        observer.join()

    def stop(self):
        """stops the flusher"""
        self._running = False
