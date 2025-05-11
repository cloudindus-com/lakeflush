import pytest
import threading
from unittest.mock import create_autospec
from lakeflush.core import Collector
from lakeflush.core import Flusher


@pytest.fixture
def collector(tmp_path):
    """collector setup"""
    collector = Collector(tmp_path, "testfile")
    _data = ",".join(__name__)
    data = _data * (1024 * 1024 // len(_data))  # 1 MB
    collector.collect(data)
    yield collector


@pytest.fixture
def mock_flush(tmp_path, mocker):
    """flusher setup"""
    flusher = Flusher(tmp_path, "testfile")
    mock_flush = create_autospec(flusher.flush, return_value="flushed")
    mocker.patch.object(flusher, "flush", new=mock_flush)
    flusher_thread = threading.Thread(target=flusher.start)
    flusher_thread.start()
    yield mock_flush
    flusher.stop()
    flusher_thread.join(timeout=1)


class TestFlusher:
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
        Test the flusher validation.
        """
        with pytest.raises(ValueError):
            Flusher(filepath, filename)

    def test_listener(self, collector, mock_flush):
        """Test that flusher listens the collect event"""

        collector.collect(__name__ * 1024)

        mock_flush.assert_called_once()
