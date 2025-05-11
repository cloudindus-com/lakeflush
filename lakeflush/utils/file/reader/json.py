class JSONFileReader:
    """Reads json fle and processes the content further"""

    def __init__(self) -> None:
        # added for common check
        self.header_data = None

    def read(self, file_path: str):
        with open(file_path, "r") as fp:
            data = fp.read()
            if data:
                yield data
