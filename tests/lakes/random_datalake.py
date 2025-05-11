import os
import time
import random
import json
import csv
import uuid
from pathlib import Path
from datetime import datetime, timedelta


def write_random_json(filepath: Path):
    """Creates random json file"""
    _id = str(uuid.uuid4())
    data = {
        "id": _id,
        "name": f"Item_{random.randint(1, 100)}",
        "price": round(random.uniform(10.0, 1000.0), 2),
        "in_stock": random.choice([True, False]),
        "tags": random.sample(
            ["electronics", "clothing", "home", "garden", "sports"],
            k=random.randint(1, 3),
        ),
        "created_at": (
            datetime.now() - timedelta(days=random.randint(0, 365))
        ).isoformat(),
        "metadata": {
            "weight": random.randint(1, 50),
            "dimensions": {
                "width": random.randint(5, 100),
                "height": random.randint(5, 100),
                "depth": random.randint(5, 50),
            },
        },
    }
    with open(filepath / f"{_id}.json", "x") as fp:
        json.dump(data, fp)


def write_random_csv(filepath: Path, num_rows=1000):
    """Creates random csv file"""

    def generate_random_data():
        """Generate random data rows"""

        departments = ["Sales", "Engineering", "Marketing", "HR", "Finance"]
        first_names = ["Abhinav", "Steve", "Mark", "Elon", "Jeff", "Sundar"]
        last_names = ["Kaurav", "Jobs", "Zuckerberg", "Musk", "Bezoss", "Pichai"]

        for i in range(1, num_rows + 1):
            first = random.choice(first_names)
            last = random.choice(last_names)
            domain = random.choice(["gmail.com", "zoho.com", "outlook.com"])

            row = [
                i,  # ID
                _id,
                first,
                last,
                f"{first.lower()}.{last.lower()}@{domain}",  # Email
                random.randint(20, 65),  # Age
                (datetime.now() - timedelta(days=random.randint(0, 365 * 5))).strftime(
                    "%Y-%m-%d"
                ),  # Join date
                round(random.uniform(30000, 120000), 2),  # Salary
                random.choice([True, False]),  # Active status
                random.choice(departments),  # Department
            ]
            yield row

    _id = str(uuid.uuid4())
    with open(filepath / f"{_id}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)

        headers = [
            "id",
            "fileid",
            "first_name",
            "last_name",
            "email",
            "age",
            "join_date",
            "salary",
            "is_active",
            "department",
        ]

        writer.writerow(headers)

        for row in generate_random_data():
            writer.writerow(row)


def create_random_datalake(
    root_dir: str,
    partition_level: int,
    starttime: datetime,
    endtime: datetime,
    file_type: str = "json",
    max_files: int = 100,
    csv_num_rows: int = 1000,
):
    """Creates random data lake using date partition range and random json files.
    Support only date based partition => year=yyyy/month=mm/day=dd/hour=hh

     Args:
        root_dir (str): The directory for data lake creation
        partition_level (int): The parition level, Max is 4.
        starttime (timedelta): The first partition time.
        endtime (timedelta): The last partition time
        file_type (str): The type of file to generate 'json' or 'csv' (default 'json').
        max_files (int): The number of random files to generate (default 100).
        csv_num_rows (int): The number of rows for csv file_type only (default 1000).
    """

    root = Path(root_dir)
    if not root_dir or not os.path.exists(root):
        raise ValueError(f"root_dir: {root_dir} does not exists.")

    if partition_level > 4:
        raise ValueError("maximum 4 partition level supported.")

    if starttime > endtime:
        raise ValueError("starttime should be less than endtime.")

    print("setting up...")

    def _get_partition_range(partition: int, _starttime: datetime):
        same_year = _starttime.year == endtime.year
        same_month = same_year and _starttime.month == endtime.month
        same_date = same_month and _starttime.day == endtime.day

        if partition == 1:
            return range(_starttime.year, endtime.year + 1)
        elif partition == 2:
            last_month = endtime.month if same_year else 12
            return range(_starttime.month, last_month + 1)
        elif partition == 3:
            if same_month:
                return range(_starttime.day, endtime.day + 1)
            # Compute last day of the month
            next_month = _starttime.replace(day=28) + timedelta(days=4)
            last_day = (next_month - timedelta(days=next_month.day)).day
            return range(_starttime.day, last_day + 1)
        else:
            return (
                range(_starttime.hour, endtime.hour + 1)
                if same_date
                else range(_starttime.hour, 24)
            )

    def add_years(dt, years=1):
        try:
            return dt.replace(year=dt.year + years)
        except ValueError:  # Leap year (Feb 29) case
            return dt.replace(year=dt.year + years, month=3, day=1) - timedelta(days=1)

    def add_months(dt, months=1):
        # Calculate new year and month
        total_months = dt.month + months
        new_year = dt.year + (total_months - 1) // 12
        new_month = (total_months - 1) % 12 + 1

        # Try to keep same day (works for most cases)
        try:
            return dt.replace(year=new_year, month=new_month)
        except ValueError:
            # For invalid days (e.g., Jan 31 → Feb), return last day of new month
            return dt.replace(year=new_year, month=new_month + 1, day=1) - timedelta(
                days=1
            )

    def add_days(dt, days=1):
        return dt + timedelta(days=days)

    def add_hours(dt, hours=1):
        return dt + timedelta(hours=hours)

    partitions = {
        1: ["%Y", "year", add_years],
        2: ["%m", "month", add_months],
        3: ["%d", "day", add_days],
        4: ["%H", "hour", add_hours],
    }

    partition_added = 0
    files_added = 0

    def _log(msg: str):
        nonlocal partition_added
        partition_added += 1
        print(f"{msg.ljust(70)}", end="\r", flush=True)

    def _create_partition(
        current_path: Path, current_partition: int, _starttime: datetime
    ):
        if current_partition == partition_level:
            # get range of partition for last level
            for partition in _get_partition_range(current_partition, _starttime):
                format, name, fn = partitions[current_partition]
                _starttime = _starttime.replace(**{name: partition})
                # create partition name format: name=value
                partition_name = f"{name}={_starttime.strftime(format)}"
                partition_path = current_path / partition_name
                _partition_path = str(partition_path).replace(str(root), "")
                os.makedirs(partition_path, exist_ok=True)
                # add files to last partition
                for f in range(max_files):
                    if file_type == "csv":
                        write_random_csv(partition_path, csv_num_rows)
                    else:
                        write_random_json(partition_path)
                nonlocal files_added
                files_added += max_files
                _log(f"added partition: {_partition_path} with {max_files} files")
            _starttime = fn(_starttime)
            if _starttime < endtime:
                _create_partition(root, 1, _starttime)
            return
        format, name, fn = partitions[current_partition]
        # create partition name format: name=value
        partition_name = f"{name}={_starttime.strftime(format)}"
        current_path = current_path / partition_name
        os.makedirs(current_path, exist_ok=True)
        _log(f"added partition: {str(current_path).replace(str(root), '')}")
        return _create_partition(current_path, current_partition + 1, _starttime)

    _time = time.time()
    print("creating datalake...")
    _create_partition(root, 1, starttime)
    _log(f"{partition_added} partition and {files_added } files added...")
    print(f"\ndatalake created... time taken: {time.time() - _time}")
