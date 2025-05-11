import pytest
from datetime import datetime, timedelta
import os
from lakeflush.utils.file import FileType
from lakeflush.collectors import LocalLakeCollector
from tests.lakes.random_datalake import create_random_datalake


@pytest.fixture
def collector_args(tmp_path):
    """collector args"""
    yield dict(filepath=tmp_path, filename="testfile")


@pytest.fixture
def collect(mocker):
    """collect mock"""
    yield mocker.patch("lakeflush.core.collector.Collector.collect")


class TestLocalLakeCollector:
    @pytest.mark.parametrize(
        "root_dir",
        [
            (""),
            ("./testpath"),
        ],
    )
    def test_validation(self, root_dir, collector_args):
        """
        Test the collector validation.
        """
        with pytest.raises(ValueError):
            LocalLakeCollector(root_dir, **collector_args)

    @pytest.mark.parametrize(
        "locallake_args",
        [
            dict(file_type="json", max_files=10),
            dict(file_type="csv", max_files=10, csv_num_rows=1),
        ],
    )
    def test_collection(self, locallake_args, collector_args, tmp_path, collect):
        """
        Test the collector process.
        """

        file_path = tmp_path / "locallake"
        os.makedirs(file_path)
        endtime = datetime.now()
        starttime = endtime + timedelta(hours=-1)
        partition_level = 4
        create_random_datalake(
            file_path,
            partition_level,
            starttime,
            endtime,
            **locallake_args,
        )
        collector = LocalLakeCollector(file_path, **collector_args)
        collector.start()

        assert collect.call_count == 20

    @pytest.mark.parametrize(
        "locallake_args",
        [
            dict(file_type="json", max_files=10),
            dict(file_type="csv", max_files=10, csv_num_rows=1),
        ],
    )
    def test_collection_pattern(
        self, locallake_args, collector_args, tmp_path, collect
    ):
        """
        Test the local lake collector collecting files using file name pattern.
        """

        file_path = tmp_path / "locallake"
        os.makedirs(file_path)
        endtime = datetime.now()
        starttime = endtime + timedelta(hours=-1)
        partition_level = 4
        create_random_datalake(
            file_path,
            partition_level,
            starttime,
            endtime,
            **locallake_args,
        )
        file_type = locallake_args["file_type"]
        if file_type == "csv":
            pattern = "*.json"
        else:
            pattern = "*.csv"
        collector = LocalLakeCollector(
            file_path, match_patterns=[pattern], **collector_args
        )
        collector.start()

        assert collect.call_count == 0

    @pytest.mark.parametrize(
        "csv_header",
        [False, True],
    )
    def test_collection_csv(self, csv_header, collector_args, tmp_path, collect):
        """
        Test the local lake collector collecting csv files with header lock.
        """

        file_path = tmp_path / "locallake"
        os.makedirs(file_path)
        endtime = datetime.now()
        starttime = endtime + timedelta(hours=-1)
        partition_level = 4
        create_random_datalake(
            file_path,
            partition_level,
            starttime,
            endtime,
            file_type="csv",
            max_files=2,
            csv_num_rows=100,
        )
        collector = LocalLakeCollector(
            file_path,
            file_type=FileType.CSV,
            csv_header=csv_header,
            **collector_args,
        )
        collector.start()

        if csv_header:
            assert collect.call_count == 5
        else:
            assert collect.call_count == 8
