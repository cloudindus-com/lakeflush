import pytest
import threading
from datetime import datetime, timedelta
import os
import time
from tests.lakes.random_datalake import create_random_datalake
from lakeflush.collectors import LocalLakeCollector
from lakeflush.flushers import LocalLakeFlusher


@pytest.fixture
def collector_args(tmp_path):
    """collector args"""
    yield dict(filepath=tmp_path, filename="testfile")


@pytest.fixture
def collector(tmp_path, collector_args):
    """collector setup"""
    file_path = tmp_path / "locallake"
    os.makedirs(file_path)
    endtime = datetime.now()
    starttime = endtime + timedelta(hours=-1)
    partition_level = 4
    create_random_datalake(
        file_path, partition_level, starttime, endtime, file_type="csv", max_files=5
    )
    collector = LocalLakeCollector(root_dir=file_path, **collector_args)
    yield collector


class TestLocalLakeFlusher:
    @pytest.mark.parametrize(
        "root_dir",
        [
            (""),
            ("./testpath"),
        ],
    )
    def test_validation(self, root_dir, collector_args):
        """
        Test the flusher validation.
        """
        with pytest.raises(ValueError):
            LocalLakeFlusher(root_dir, **collector_args)

    def test_flush(self, collector, collector_args, tmp_path):
        """Test that local lake flusher flushes the collected file"""

        file_path = tmp_path / "locallakeflush"
        os.makedirs(file_path)
        flusher = LocalLakeFlusher(root_dir=file_path, **collector_args)
        flusher_thread = threading.Thread(target=flusher.start)
        try:
            flusher_thread.start()
            collector.start()
            time.sleep(0.01)
            file_paths = list(file_path.glob("testfile.*.lakeflush"))

            assert len(file_paths) == 1

        finally:
            flusher.stop()
            flusher_thread.join(timeout=1)

    def test_flush_partition(self, collector, collector_args, tmp_path):
        """Test that local lake flusher flushes the colleced file with date partiton"""

        file_path = tmp_path / "locallakeflush"
        os.makedirs(file_path)
        flusher = LocalLakeFlusher(
            root_dir=file_path,
            date_partition_format="date=%Y-%m-%d/hour=%H",
            **collector_args,
        )
        flusher_thread = threading.Thread(target=flusher.start)
        try:
            flusher_thread.start()
            collector.start()
            time.sleep(0.01)
            date_partition = datetime.now().strftime("%Y-%m-%d")
            file_paths = list(
                file_path.glob(f"date={date_partition}/hour=*/testfile.*.lakeflush")
            )

            assert len(file_paths) == 1

        finally:
            flusher.stop()
            flusher_thread.join(timeout=1)
