import time


class CSVFileReader:
    """Reads csv file and processes the content further"""

    def __init__(self, header: bool, batch_size=100) -> None:
        self.header = header
        self.header_data = None
        self.batch_size = batch_size

    def read(self, file_path: str):
        with open(file_path, "r") as file:
            if self.header:
                if not self.header_data:
                    # Store header
                    self.header_data = next(file).strip()
                    yield self.header_data
                else:
                    # Skip header
                    next(file)

            # Process in batch_size
            batch = []
            for line in file:
                batch.append(line)
                if len(batch) >= self.batch_size:
                    yield "".join(batch).strip()
                    batch = []
                    # relax CPU
                    time.sleep(0.001)

            if batch:
                yield "".join(batch).strip()
