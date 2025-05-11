from watchdog.events import FileSystemEventHandler


class FileRotationEventHandler(FileSystemEventHandler):
    """Handles event of files collected by lakeflush collector"""

    def __init__(self, keyword):
        self.keyword = keyword

    def on_moved(self, event):
        if self.keyword in event.dest_path:
            self.on_collected(event.dest_path)

    def on_collected(self, dest_path: bytes | str):
        raise NotImplementedError
