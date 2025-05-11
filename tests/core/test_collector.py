import pytest
import time
import gzip
import os
from pathlib import Path
from lakeflush.core import Collector


@pytest.fixture(autouse=True)
def mock_time(monkeypatch):
    """Fixture to mock time functions"""
    current_time = time.time()

    def mock_time_func():
        return current_time

    def mock_sleep(seconds):
        nonlocal current_time
        current_time += seconds

    monkeypatch.setattr(time, "time", mock_time_func)
    monkeypatch.setattr(time, "sleep", mock_sleep)

    return mock_time_func


class TestCollector:
    @pytest.mark.parametrize(
        "filepath,filename",
        [
            (None, None),
            ("", ""),
            (None, "testfile"),
            ("", "testfile"),
            ("./testpath", None),
            ("./testpath", ""),
            ("./testpath1", "testname"),
        ],
    )
    def test_validation(self, filepath, filename):
        """
        Test the collector validation.
        """
        with pytest.raises(ValueError):
            Collector(filepath, filename)

    @pytest.mark.parametrize(
        "collector_kwargs",
        [{}, {"compress": True}],
    )
    def test_initiaization(self, collector_kwargs, tmp_path: Path):
        """Test that collector initialization"""
        Collector(tmp_path, "testfile", **collector_kwargs)
        if collector_kwargs.get("compress") is True:
            file_path = tmp_path / "testfile.lakeflush.inprogress.gz"
        else:
            file_path = tmp_path / "testfile.lakeflush.inprogress"

        assert file_path.exists()
        assert os.path.getsize(file_path) == 0

    @pytest.mark.parametrize(
        "collector_kwargs",
        [{}, {"compress": True}],
    )
    def test_collection(self, collector_kwargs, tmp_path: Path):
        """Test that collector collecting data"""
        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        if collector_kwargs.get("compress") is True:
            file_path = tmp_path / "testfile.lakeflush.inprogress.gz"
            _open = gzip.open
        else:
            file_path = tmp_path / "testfile.lakeflush.inprogress"
            _open = open

        files_data = [
            ",".join(self.__class__.__name__),
            "|".join(self.__class__.__name__),
        ]
        for data in files_data:
            collector.collect(data)

        assert os.path.getsize(file_path) > 0

        with _open(file_path, "rt") as f:
            lines = [f.readline(), f.readline()]

        assert lines[0].strip() == files_data[0]
        assert lines[1].strip() == files_data[1]

    @pytest.mark.parametrize(
        "collector_kwargs",
        [{"max_size_mb": 1}, {"max_size_mb": 2}],
    )
    def test_collection_by_size(self, collector_kwargs, tmp_path: Path):
        """Test that collector collected data in file using max_size_mb attribute"""

        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        file_size = collector_kwargs["max_size_mb"] * 1024 * 1024  # MB
        current_size = 0
        _data = ",".join(self.__class__.__name__)
        data = _data * (1024 // len(_data))  # 1 KB
        while current_size < file_size:
            collector.collect(data)
            current_size += len(data)

        file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected"))

        assert len(file_paths) == 1
        assert abs(os.path.getsize(file_paths[0]) - file_size) <= 1024

    @pytest.mark.parametrize(
        "collector_kwargs",
        [
            {"max_time_mins": 2},
        ],
    )
    def test_collection_by_time(self, collector_kwargs, tmp_path: Path):
        """Test that collector collected data in file using max_time_mins attribute"""

        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        total_secs = collector_kwargs["max_time_mins"] * 60
        _data = ",".join(self.__class__.__name__)
        data = _data * (1024 // len(_data))  # 1 KB
        current_time = time.time()
        file_time = time.time() + total_secs
        while current_time < file_time:
            collector.collect(data)
            time.sleep(10)
            current_time = time.time()

        collector.collect(data)
        file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected"))

        assert len(file_paths) == 1

    @pytest.mark.parametrize(
        "collector_kwargs",
        [
            {"max_size_mb": 2, "compress": True},
        ],
    )
    def test_gzip_collection_by_size(self, collector_kwargs, tmp_path: Path):
        """Test that collector collected data in gzip file using max_size_mb attr"""

        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        max_size_mb = collector_kwargs["max_size_mb"]
        file_size = max_size_mb * 1024 * 1024  # MB
        _data = ",".join(self.__class__.__name__)
        data = _data * (1024 // len(_data))  # 1 KB
        file_path = tmp_path / "testfile.lakeflush.inprogress.gz"
        while os.path.getsize(file_path) < file_size:
            file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected.gz"))
            if len(file_paths) > 0:
                break
            current_size = 0
            while current_size < file_size:
                collector.collect(data)
                current_size += len(data)

        collector.collect(data)
        file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected.gz"))

        assert len(file_paths) == 1
        assert abs(os.path.getsize(file_paths[0]) - file_size) <= max_size_mb * 1024

    @pytest.mark.parametrize(
        "collector_kwargs",
        [
            {"max_time_mins": 1, "compress": True},
        ],
    )
    def test_gzip_collection_by_time(self, collector_kwargs, tmp_path: Path):
        """Test that collector collected data in gz file using max_time_mins attr"""

        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        max_time_mins = collector_kwargs["max_time_mins"]
        _data = ",".join(self.__class__.__name__)
        data = _data * (1024 // len(_data))  # 1 KB
        current_time = time.time()
        file_time = time.time() + max_time_mins * 60
        while current_time < file_time:
            collector.collect(data)
            time.sleep(10)
            current_time = time.time()

        collector.collect(data)
        file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected.gz"))

        assert len(file_paths) == 1

    @pytest.mark.parametrize(
        "collector_kwargs",
        [{"max_size_mb": 1, "max_time_mins": 60}],
    )
    def test_collection_mutiple(self, collector_kwargs, tmp_path: Path):
        """Test that collector collected multiples file using max_size_mb attribute"""

        collector = Collector(tmp_path, "testfile", **collector_kwargs)
        max_size_mb = collector_kwargs["max_size_mb"]
        _data = ",".join(self.__class__.__name__)
        data = _data * (max_size_mb * 1024 * 1024 // len(_data))  # 1 MB
        collector.collect(data)  # 1
        collector.collect(data)  # 2
        collector.collect(data)  # 3

        file_paths = list(tmp_path.glob("testfile.*.lakeflush.collected"))

        assert len(file_paths) == 3
