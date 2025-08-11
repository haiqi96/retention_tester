from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from contextlib import closing
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, List, Optional, Set

import yaml
from clp_package_utils.general import (
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH,
    get_clp_home,
    validate_and_load_db_credentials_file,
)
from clp_py_utils.clp_config import (
    ArchiveOutput,
    CLP_DEFAULT_DATASET_NAME,
    CLPConfig,
    Database,
    StorageType,
)
from clp_py_utils.clp_metadata_db_utils import fetch_existing_datasets, get_archives_table_name
from clp_py_utils.core import read_yaml_config_file
from clp_py_utils.sql_adapter import SQL_Adapter
from job_orchestration.garbage_collector.constants import MIN_TO_SECONDS, SECOND_TO_MILLISECOND

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

# Add a console handler
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

LOG_TS_DELTA: Final[float] = 10 / SECOND_TO_MILLISECOND
TS_FORMAT_STRING: Final[str] = "%Y-%m-%d %H:%M:%S,%f"


@dataclass
class RetentionConfig:
    retention_period_minutes: int
    sweep_interval_minutes: int


def load_config_file(clp_home: Path):
    config_file_path = clp_home / "etc" / "clp-config.yml"
    if config_file_path.exists():
        raw_clp_config = read_yaml_config_file(config_file_path)
        if raw_clp_config is None:
            clp_config = CLPConfig()
        else:
            clp_config = CLPConfig.parse_obj(raw_clp_config)
    else:
        raise ValueError(f"Config file '{config_file_path}' does not exist.")

    return clp_config


def start_clp(clp_home: Path, config_path: Path):
    start_clp_except_garbage_collector(clp_home, config_path)
    start_garbage_collector(clp_home, config_path)


def start_clp_except_garbage_collector(clp_home: Path, config_path: Path):
    # Perhaps we can also patch the start_clp.py later, but for now, use a hardcode list
    components = [
        "database",
        "queue",
        "redis",
        "results_cache",
        "compression_scheduler",
        "query_scheduler",
        "compression_worker",
        "query_worker",
        "reducer",
        "webui",
    ]
    for component in components:
        cmd = [str(clp_home / "sbin/start-clp.sh"), "--config", str(config_path), component]
        subprocess.run(cmd, cwd=clp_home, check=True)


def restart_garbage_collector(clp_home: Path, config_path: Path):
    cmd = [
        str(clp_home / "sbin/stop-clp.sh"),
        "-f",
        "--config",
        str(config_path),
        "garbage_collector",
    ]
    subprocess.run(cmd, cwd=clp_home, check=True)
    start_garbage_collector(clp_home, config_path)


def start_garbage_collector(clp_home: Path, config_path: Path):
    cmd = [str(clp_home / "sbin/start-clp.sh"), "--config", str(config_path), "garbage_collector"]
    subprocess.run(cmd, cwd=clp_home, check=True)
    time.sleep(10)


def clear_package_storage(clp_config: CLPConfig):
    if clp_config.logs_directory.exists():
        shutil.rmtree(clp_config.logs_directory)
    if clp_config.data_directory.exists():
        shutil.rmtree(clp_config.data_directory)


def execute_compression(clp_home: Path, input_log: Path, timestamp_key: Optional[str] = None):
    cmd = [str(clp_home / "sbin/compress.sh"), str(input_log)]
    if timestamp_key is not None:
        cmd.extend(["--timestamp-key", timestamp_key])
    subprocess.run(cmd, check=True)


def stop_clp(clp_home: Path):
    cmd = [str(clp_home / "sbin/stop-clp.sh"), "-f"]
    subprocess.run(cmd, check=True)


def package_cleanup(clp_home: Path, clp_config: CLPConfig):
    stop_clp(clp_home)
    clear_package_storage(clp_config)


def find_archives_in_metadata_database(
    clp_home: Path, clp_config: CLPConfig, dataset: Optional[str] = None
) -> Set[str]:
    validate_and_load_db_credentials_file(clp_config, clp_home, True)
    database_config = clp_config.database

    sql_adapter: SQL_Adapter = SQL_Adapter(database_config)
    clp_db_connection_params: dict[str, any] = database_config.get_clp_connection_params_and_type(
        True
    )
    table_prefix: str = clp_db_connection_params["table_prefix"]

    with closing(sql_adapter.create_connection(True)) as db_conn, closing(
        db_conn.cursor(dictionary=True)
    ) as db_cursor:
        if dataset is not None and dataset not in fetch_existing_datasets(db_cursor, table_prefix):
            raise ValueError(f"Dataset {dataset} doesn't exist")

        query = f"""SELECT id as archive_id
                FROM {get_archives_table_name(table_prefix, dataset)}
                """
        db_cursor.execute(query)
        archives_set = {item["archive_id"] for item in db_cursor.fetchall()}
        return archives_set


def find_archives_on_disk(
    archive_output_config: ArchiveOutput, dataset: Optional[str] = None
) -> Set[str]:
    storage_type = archive_output_config.storage.type
    if StorageType.FS != archive_output_config.storage.type:
        raise ValueError(f"Unexpected storage type {storage_type}")

    archive_directory = archive_output_config.get_directory()
    if dataset is not None:
        archive_directory = archive_directory / dataset

    archives_set = {item.name for item in archive_directory.iterdir()}
    return archives_set


def configure_retention(
    base_clp_config: CLPConfig,
    archive_config: Optional[RetentionConfig] = None,
    search_result_config: Optional[RetentionConfig] = None,
) -> CLPConfig:
    retention_test_config = deepcopy(base_clp_config)

    garbage_collector_config = retention_test_config.garbage_collector
    garbage_collector_config.logging_level = "DEBUG"

    if archive_config is not None:
        retention_test_config.archive_output.retention_period = (
            archive_config.retention_period_minutes
        )
        garbage_collector_config.sweep_interval.archive = archive_config.sweep_interval_minutes

    if search_result_config is not None:
        retention_test_config.results_cache.retention_period = (
            search_result_config.retention_period_minutes
        )
        garbage_collector_config.sweep_interval.search_result = (
            search_result_config.sweep_interval_minutes
        )

    return retention_test_config


def get_formatted_utc_ts(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(TS_FORMAT_STRING)[:-3]


def generate_unstructured_log(log_path: Path, reference_sec_epoch: Optional[float] = None) -> None:
    log_ts = reference_sec_epoch
    log_template = "This is an example log message with id={}\n"

    with open(log_path, "w") as output:
        for i in range(100):
            log_msg = log_template.format(i)
            if log_ts is not None:
                log_ts += LOG_TS_DELTA
                log_msg = f"{get_formatted_utc_ts(log_ts)} {log_msg}"
            output.write(log_msg)


def generate_structured_log(log_path: Path, reference_sec_epoch: Optional[float]) -> None:
    log_ts = reference_sec_epoch
    json_log_template = {
        "msg": "Example log message",
    }

    with open(log_path, "w") as output:
        for i in range(100):
            json_log_template["id"] = i
            if log_ts is not None:
                log_ts += LOG_TS_DELTA
                json_log_template["timestamp"] = get_formatted_utc_ts(log_ts)
            output.write(json.dumps(json_log_template, indent=None))
            output.write("\n")


def validate_archive_matches(
    clp_home: Path, clp_config: CLPConfig, expected_cnt: Optional[int] = None
):
    archives_on_disk = find_archives_on_disk(clp_config.archive_output)
    archives_in_db = find_archives_in_metadata_database(clp_home, clp_config)

    num_archives_on_disk = len(archives_on_disk)
    num_archives_in_db = len(archives_in_db)
    if num_archives_on_disk != num_archives_in_db:
        raise ValueError(
            f"Archive counts don't match: {num_archives_on_disk}!={num_archives_in_db}"
        )

    if expected_cnt is not None and num_archives_on_disk != expected_cnt:
        raise ValueError(
            f"Expected count don't match, expected {expected_cnt}, found {num_archives_on_disk}"
        )

    mismatched_archive = [archive for archive in archives_on_disk if archive not in archives_in_db]
    if len(mismatched_archive) != 0:
        raise ValueError(f"Archive mismatch: {mismatched_archive}")


def test_clp_basic_functionality(clp_home: Path, base_clp_config: CLPConfig):
    # Test that retention cleaner works as expected.
    # Configure retention settings.
    retention_minutes = 4
    sweep_intervals = int(retention_minutes / 2)
    retention_seconds = retention_minutes * MIN_TO_SECONDS

    retention_config = RetentionConfig(retention_minutes, sweep_intervals)
    clp_config = configure_retention(base_clp_config, archive_config=retention_config)
    clp_config_path = clp_home / "etc" / "test-config.yml"
    with open(clp_config_path, "w") as f:
        yaml.safe_dump(clp_config.dump_to_primitive_dict(), f)
    # Hack: this is to make sure later ones work
    clp_config.make_config_paths_absolute(clp_home)

    package_cleanup(clp_home, clp_config)
    start_clp_except_garbage_collector(clp_home, clp_config_path)

    # Generate test logs
    log1_path = Path("test1.log").resolve()
    log2_path = Path("test2.log").resolve()

    curr_ts = datetime.now(timezone.utc).timestamp()
    generate_unstructured_log(log1_path, curr_ts - retention_seconds)
    generate_unstructured_log(log2_path, curr_ts)

    # Test log1 won't be searched.
    execute_compression(clp_home, log1_path)
    if count_search_results(clp_home) != 0:
        raise ValueError("Log1 unexpectedly generated search results")

    # Confirm that log2 shall be searched
    execute_compression(clp_home, log2_path)
    if count_search_results(clp_home) == 0:
        raise ValueError("Log2 doesn't generate any search result")

    # Since gc hasn't started, expect 2
    logger.info("Validate that there are 2 archives")
    validate_archive_matches(clp_home, clp_config, expected_cnt=2)

    # Start garbage collector
    start_garbage_collector(clp_home, clp_config_path)
    logger.info("Validate 1 archive gets removed")
    validate_archive_matches(clp_home, clp_config, expected_cnt=1)

    logger.info(f"Sleeping for {retention_seconds} seconds")
    time.sleep(retention_seconds)
    validate_archive_matches(clp_home, clp_config, expected_cnt=0)
    if count_search_results(clp_home) != 0:
        raise ValueError("Unexpectedly see search results")

    logger.info(f"Test tear down")
    os.unlink(log1_path)
    os.unlink(log2_path)
    package_cleanup(clp_home, base_clp_config)
    os.unlink(clp_config_path)
    return


def test_0_timestamp(clp_home: Path, base_clp_config: CLPConfig):
    # Test that retention cleaner does not clean up file with 0 timestamp.

    # Configure retention settings.
    retention_minutes = 2
    sweep_intervals = int(retention_minutes / 1)
    retention_seconds = retention_minutes * MIN_TO_SECONDS

    retention_config = RetentionConfig(retention_minutes, sweep_intervals)
    clp_config = configure_retention(base_clp_config, archive_config=retention_config)
    clp_config_path = clp_home / "etc" / "test-config.yml"
    with open(clp_config_path, "w") as f:
        yaml.safe_dump(clp_config.dump_to_primitive_dict(), f)
    # Hack: this is to make sure later ones work
    clp_config.make_config_paths_absolute(clp_home)

    package_cleanup(clp_home, clp_config)
    start_clp_except_garbage_collector(clp_home, clp_config_path)

    # Generate test logs
    log_path = Path("test_0_ts.log").resolve()
    generate_unstructured_log(log_path, reference_sec_epoch=None)

    # Compress the log and starts garbage collection
    execute_compression(clp_home, log_path)
    start_garbage_collector(clp_home, clp_config_path)

    # Ensure that archive is not cleaned
    logger.info("Validate that there is 1 archive")
    if count_search_results(clp_home) == 0:
        raise ValueError("No search results found")
    validate_archive_matches(clp_home, clp_config, expected_cnt=1)

    logger.info(f"Test tear down")
    os.unlink(log_path)
    package_cleanup(clp_home, base_clp_config)
    os.unlink(clp_config_path)
    return


def count_search_results(clp_home) -> int:
    cmd = [str(clp_home / "sbin/search.sh"), "*"]
    proc = subprocess.run(cmd, check=True, capture_output=True)
    search_results = proc.stdout.decode("utf-8")
    return len(search_results.split("\n")) - 1


def main(argv: List[str]) -> int:
    clp_home: Path = get_clp_home()
    # Validate and load config file
    base_clp_config: CLPConfig
    try:
        base_clp_config = load_config_file(clp_home)
    except Exception:
        logger.exception("Failed to load config.")
        return -1

    # test_clp_basic_functionality(clp_home, base_clp_config)
    test_0_timestamp(clp_home, base_clp_config)
    # text_log_path = Path("test.log").resolve()
    # json_log_path = Path("test.jsonl")
    # test_begin_ts = datetime.now(timezone.utc).timestamp()
    # adjusted_log_ts = test_begin_ts - UTC_OFFSET_SECONDS
    # generate_unstructured_log(text_log_path, test_begin_ts)
    # execute_compression(clp_home, text_log_path)
    # #
    # # json_log_path = Path("test.jsonl").resolve()
    # # execute_compression(clp_home, json_log_path, "timestamp")
    #
    # print(count_search_results(clp_home))
    #
    # retention_test_config.make_config_paths_absolute(clp_home)
    # validate_and_load_db_credentials_file(retention_test_config, clp_home, True)
    # archives = find_archives_in_metadata_database(
    #     retention_test_config.database, CLP_DEFAULT_DATASET_NAME
    # )
    # print(archives)
    # print(find_archives_on_disk(retention_test_config.archive_output, CLP_DEFAULT_DATASET_NAME))
    # stop_clp(clp_home)
    # clear_package_storage(retention_test_config)
    # text_log_path = Path("test.log")
    # json_log_path = Path("test.jsonl")
    # test_begin_ts = datetime.now().timestamp()
    # generate_unstructured_log(text_log_path, test_begin_ts)
    # generate_structured_log(json_log_path, test_begin_ts)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
