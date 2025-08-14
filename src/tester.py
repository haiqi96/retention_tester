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
from typing import Final, List, Optional, Set, Tuple

import yaml
from clp_package_utils.general import (
    get_clp_home,
    validate_and_load_db_credentials_file,
)
from clp_py_utils.clp_config import (
    ArchiveOutput,
    CLP_DEFAULT_DATASET_NAME,
    CLPConfig,
    StorageEngine,
    StorageType,
)
from clp_py_utils.clp_metadata_db_utils import fetch_existing_datasets, get_archives_table_name
from clp_py_utils.core import read_yaml_config_file
from clp_py_utils.sql_adapter import SQL_Adapter
from job_orchestration.garbage_collector.constants import MIN_TO_SECONDS, SECOND_TO_MILLISECOND
from retention_tester_utils import patch_package, revert_patch

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

LOG_TS_DELTA: Final[float] = 10 / SECOND_TO_MILLISECOND
TS_FORMAT_STRING: Final[str] = "%Y-%m-%d %H:%M:%S,%f"
NUM_LOG_LINE: Final[int] = 100
TIME_STAMP_KEY: Final[str] = "timestamp"


@dataclass
class RetentionConfig:
    retention_period_minutes: int
    sweep_interval_minutes: int


def get_clp_repo_root():
    script_path = Path(__file__)
    potential_clp_repo = script_path.parent.parent.parent.resolve()
    if not (potential_clp_repo / "components").exists():
        raise ValueError(f"{potential_clp_repo} is not a clp_repo, give up easily")

    return potential_clp_repo


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
        subprocess.run(
            cmd, cwd=clp_home, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    logger.info("Started CLP package except garbage collector...")


def restart_garbage_collector(clp_home: Path, config_path: Path):
    cmd = [
        str(clp_home / "sbin/stop-clp.sh"),
        "-f",
        "--config",
        str(config_path),
        "garbage_collector",
    ]
    subprocess.run(
        cmd, cwd=clp_home, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    logger.info("Stopped garbage collector...")
    start_garbage_collector(clp_home, config_path)


def start_garbage_collector(clp_home: Path, config_path: Path):
    cmd = [str(clp_home / "sbin/start-clp.sh"), "--config", str(config_path), "garbage_collector"]
    subprocess.run(
        cmd, cwd=clp_home, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    logger.info("Started garbage collector...")
    # Slightly wait for garbage collector to do its job
    time.sleep(10)


def clear_package_storage(clp_config: CLPConfig):
    if clp_config.logs_directory.exists():
        shutil.rmtree(clp_config.logs_directory)
    if clp_config.data_directory.exists():
        shutil.rmtree(clp_config.data_directory)
    logger.info("Cleaned up package storage...")


def execute_compression(clp_home: Path, clp_config_path: Path, input_log: Path, use_clp_s: bool):
    # Note: this could silently fail
    cmd = [str(clp_home / "sbin/compress.sh"), str(input_log), "--config", str(clp_config_path)]
    if use_clp_s:
        cmd.extend(["--timestamp-key", f"{TIME_STAMP_KEY}"])

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"Compressed {input_log}")


def stop_clp(clp_home: Path):
    cmd = [str(clp_home / "sbin/stop-clp.sh"), "-f"]

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info("Stopped CLP package...")


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
    log_template = "This is an example log message with id={}\n"

    with open(log_path, "w") as output:
        for i in range(NUM_LOG_LINE):
            log_msg = log_template.format(i)
            if reference_sec_epoch is not None:
                log_ts = reference_sec_epoch - (NUM_LOG_LINE - (i+1)) * LOG_TS_DELTA
                log_msg = f"{get_formatted_utc_ts(log_ts)} {log_msg}"
            output.write(log_msg)


def generate_structured_log(log_path: Path, reference_sec_epoch: Optional[float]) -> None:
    json_log_template = {
        "msg": "Example log message",
    }

    with open(log_path, "w") as output:
        for i in range(NUM_LOG_LINE):
            json_log_template["id"] = i
            if reference_sec_epoch is not None:
                log_ts = reference_sec_epoch - (NUM_LOG_LINE - (i+1)) * LOG_TS_DELTA
                json_log_template[TIME_STAMP_KEY] = get_formatted_utc_ts(log_ts)
            output.write(json.dumps(json_log_template, indent=None))
            output.write("\n")


def validate_archive_matches(
    clp_home: Path, clp_config: CLPConfig, expected_cnt: int, dataset: Optional[str]
):
    archives_on_disk = find_archives_on_disk(clp_config.archive_output, dataset)
    archives_in_db = find_archives_in_metadata_database(clp_home, clp_config, dataset)

    num_archives_on_disk = len(archives_on_disk)
    num_archives_in_db = len(archives_in_db)
    if num_archives_on_disk != num_archives_in_db:
        raise ValueError(
            f"Archive counts don't match: {num_archives_on_disk}!={num_archives_in_db}"
        )

    if num_archives_on_disk != expected_cnt:
        raise ValueError(
            f"Expected count don't match, expected {expected_cnt}, found {num_archives_on_disk}"
        )

    mismatched_archive = [archive for archive in archives_on_disk if archive not in archives_in_db]
    if len(mismatched_archive) != 0:
        raise ValueError(f"Archive mismatch: {mismatched_archive}")


def generate_log(log_path: Path, use_clp_s: bool, reference_sec_epoch: Optional[float]):
    if use_clp_s:
        generate_structured_log(log_path, reference_sec_epoch)
    else:
        generate_unstructured_log(log_path, reference_sec_epoch)


def test_basic_functionality(clp_home: Path, base_clp_config: CLPConfig):
    logger.info("Test basic retention")
    # Test that retention cleaner works as expected.
    # Configure retention settings.
    retention_minutes = 4
    sweep_intervals = int(retention_minutes / 2)
    retention_seconds = retention_minutes * MIN_TO_SECONDS

    # clp_s or clp
    use_clp_s, dataset = get_package_type(base_clp_config)

    retention_config = RetentionConfig(retention_minutes, sweep_intervals)
    clp_config = configure_retention(base_clp_config, archive_config=retention_config)
    clp_config_path = clp_home / "etc" / "test-config.yml"
    with open(clp_config_path, "w") as f:
        yaml.safe_dump(clp_config.dump_to_primitive_dict(), f)
    # Hack: this is to make sure later ones work
    clp_config.make_config_paths_absolute(clp_home)

    stop_clp(clp_home)
    clear_package_storage(clp_config)

    start_clp_except_garbage_collector(clp_home, clp_config_path)

    # Generate test logs
    log1_path = Path("test1.log").resolve()
    log2_path = Path("test2.log").resolve()

    curr_ts = datetime.now(timezone.utc).timestamp()
    generate_log(log1_path, use_clp_s, curr_ts - retention_seconds)
    generate_log(log2_path, use_clp_s, curr_ts)

    # Test log1 won't be searched.
    execute_compression(clp_home, clp_config_path, log1_path, use_clp_s)
    validate_search_results(clp_home, clp_config_path, 0, dataset)

    # Confirm that log2 shall be searched
    execute_compression(clp_home, clp_config_path, log2_path, use_clp_s)
    validate_search_results(clp_home, clp_config_path, NUM_LOG_LINE, dataset)

    # Since gc hasn't started, expect 2 archives
    logger.info("Validate that there are 2 archives")
    validate_archive_matches(clp_home, clp_config, 2, dataset)

    # Start garbage collector
    start_garbage_collector(clp_home, clp_config_path)
    logger.info("Validate 1 archive gets removed")
    validate_archive_matches(clp_home, clp_config, 1, dataset)

    logger.info(f"Sleeping for {retention_seconds} seconds")
    time.sleep(retention_seconds)
    validate_archive_matches(clp_home, clp_config, 0, dataset)
    validate_search_results(clp_home, clp_config_path, 0, dataset)

    # test logs with 0 timestamp
    logger.info("Test logs without timestamp")
    # Compress the log and immediately restart garbage collection
    log_0_path = Path("test_0_ts.log").resolve()
    generate_log(log_0_path, use_clp_s, None)
    execute_compression(clp_home, clp_config_path, log_0_path, use_clp_s)
    restart_garbage_collector(clp_home, clp_config_path)

    # Ensure that archive is not cleaned
    logger.info("Validate that there is 1 archive")
    validate_search_results(clp_home, clp_config_path, NUM_LOG_LINE, dataset)
    validate_archive_matches(clp_home, clp_config, 1, dataset)

    logger.info(f"Test passed")
    os.unlink(log1_path)
    os.unlink(log2_path)
    stop_clp(clp_home)
    clear_package_storage(clp_config)
    os.unlink(clp_config_path)
    return


def get_package_type(clp_config: CLPConfig) -> Tuple[bool, Optional[str]]:
    use_clp_s = clp_config.package.storage_engine == StorageEngine.CLP_S
    dataset: Optional[str] = CLP_DEFAULT_DATASET_NAME if use_clp_s else None

    logger.info(f"Test {clp_config.package.storage_engine}")
    return use_clp_s, dataset


def test_fault_tolerance(clp_home: Path, base_clp_config: CLPConfig):
    logger.info("Test fault tolerance")
    # Configure retention settings.
    retention_minutes = 2
    sweep_intervals = int(retention_minutes / 1)
    retention_seconds = retention_minutes * MIN_TO_SECONDS

    # clp_s or clp
    use_clp_s, dataset = get_package_type(base_clp_config)

    retention_config = RetentionConfig(retention_minutes, sweep_intervals)
    clp_config = configure_retention(base_clp_config, archive_config=retention_config)
    clp_config_path = clp_home / "etc" / "test-config.yml"
    with open(clp_config_path, "w") as f:
        yaml.safe_dump(clp_config.dump_to_primitive_dict(), f)

    stop_clp(clp_home)
    clear_package_storage(clp_config)

    # Patch the package with test patch
    clp_repo_root = get_clp_repo_root()
    fault_tolerance_patch = Path("patch") / "fault_tolerance.patch"
    patch_package(clp_home, clp_repo_root, fault_tolerance_patch)
    try:
        start_clp_except_garbage_collector(clp_home, clp_config_path)

        # Generate test logs
        log_path = Path("test.log").resolve()
        curr_ts = datetime.now(timezone.utc).timestamp()
        generate_log(log_path, use_clp_s, curr_ts - retention_seconds)

        # Compress the log and starts garbage collection
        execute_compression(clp_home, clp_config_path, log_path, use_clp_s)
        start_garbage_collector(clp_home, clp_config_path)

        # Ensure that archive is only cleaned in the metadata database but not from disk
        archives_in_db = find_archives_in_metadata_database(clp_home, clp_config, dataset)
        if len(archives_in_db) != 0:
            raise ValueError(f"unexpected number of archives in db: {len(archives_in_db)}")

        archives_on_disk = find_archives_on_disk(clp_config.archive_output, dataset)
        if len(archives_on_disk) != 1:
            raise ValueError(f"unexpected number of archives on disk: {len(archives_in_db)}")

        revert_patch(clp_home, clp_repo_root, fault_tolerance_patch)
    except Exception:
        revert_patch(clp_home, clp_repo_root, fault_tolerance_patch)
        raise

    logger.info(f"Restart garbage collector without instrumentation")
    restart_garbage_collector(clp_home, clp_config_path)
    validate_archive_matches(clp_home, clp_config, 0, dataset)

    logger.info(f"Test passed")
    stop_clp(clp_home)
    clear_package_storage(clp_config)
    os.unlink(clp_config_path)
    os.unlink(log_path)

    return


def sleep_with_log(sleep_sec: float) -> None:
    logger.info(f"Sleep for {sleep_sec} seconds")
    time.sleep(sleep_sec)


def test_race_condition(clp_home, base_clp_config):
    logger.info("Test race condition")
    # Configure retention settings.
    retention_minutes = 4
    sweep_interval = int(retention_minutes / 2)
    retention_seconds = retention_minutes * MIN_TO_SECONDS
    sweep_interval_seconds = sweep_interval * MIN_TO_SECONDS
    # This is a hardcoded value that should agree with the patch
    search_sleep_seconds = 60

    # clp_s or clp
    use_clp_s, dataset = get_package_type(base_clp_config)

    retention_config = RetentionConfig(retention_minutes, sweep_interval)
    clp_config = configure_retention(base_clp_config, archive_config=retention_config)
    clp_config_path = clp_home / "etc" / "test-config.yml"
    with open(clp_config_path, "w") as f:
        yaml.safe_dump(clp_config.dump_to_primitive_dict(), f)
    # Hack: this is to make sure later ones work
    clp_config.make_config_paths_absolute(clp_home)

    stop_clp(clp_home)
    clear_package_storage(clp_config)

    # Patch the package with test patch
    clp_repo_root = get_clp_repo_root()
    race_condition_patch = Path("patch") / "race_condition.patch"
    patch_package(clp_home, clp_repo_root, race_condition_patch)

    try:
        start_clp_except_garbage_collector(clp_home, clp_config_path)

        # Generate test logs
        log_path = Path("test.log").resolve()
        generate_log(log_path, use_clp_s, datetime.now(timezone.utc).timestamp())

        # Compress the log and start garbage collection
        execute_compression(clp_home, clp_config_path, log_path, use_clp_s)
        start_garbage_collector(clp_home, clp_config_path)

        # Sleep for TTL - S/2 seconds
        sleep_with_log(retention_seconds - search_sleep_seconds / 2)

        # start a search process that will block
        logger.info("Launch search job")
        search_cmd = get_search_cmd(clp_home, clp_config_path, dataset)
        proc = subprocess.Popen(search_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Sleep another S/2 seconds, at this point, at least TTL seconds has elapsed since
        # garbage_collector starts, such that:
        # 1. garbage collector should have waked up another two times.
        # 1. Archive should be considered out-of-dated
        # TODO: parse archive garbage collector log to verify
        sleep_with_log(search_sleep_seconds / 2)
        logger.info("verified that search hasn't finished")
        if proc.poll() is not None:
            raise RuntimeError("Search job finished unexpected early")

        # At this point verify that the archive isn't removed by the garbage collector yet because
        # of the running search job.
        validate_archive_matches(clp_home, clp_config, 1, dataset)

        # Verify that search don't return results (it won't hang because query scheduler will skip
        # the only archive)
        cur_time = time.time()
        validate_search_results(clp_home, clp_config_path, 0, dataset)
        search_time = time.time() - cur_time
        if search_time > 10:
            raise ValueError(f"Search job unexpectedly took {search_time} to finish")

        # Sleep another S/2 seconds and some room for search job to finish
        sleep_with_log(search_sleep_seconds / 2 + 10)
        logger.info("verified that search has finished")
        if proc.poll() is None:
            raise RuntimeError("Search job should have finished")
        if proc.returncode != 0:
            raise ValueError(f"Search job returned with {proc.returncode}")
        stdout, _ = proc.communicate()
        search_results_count = len(stdout.decode("utf-8").split("\n")) - 1
        if search_results_count != NUM_LOG_LINE:
            raise ValueError(f"Unexpected number of search results: {search_results_count}")

        # Sleep for F seconds, which should guarantee the garbage collector to wake up and remove
        # the archive
        sleep_with_log(sweep_interval_seconds)
        logger.info("Verify archive is removed")
        validate_archive_matches(clp_home, clp_config, 0, dataset)
        revert_patch(clp_home, clp_repo_root, race_condition_patch)
    except Exception:
        revert_patch(clp_home, clp_repo_root, race_condition_patch)
        raise

    logger.info(f"Test passed")
    stop_clp(clp_home)
    clear_package_storage(clp_config)
    os.unlink(log_path)
    os.unlink(clp_config_path)


def validate_search_results(
    clp_home: Path, clp_config_path: Path, expected_cnt: int, dataset: Optional[str]
) -> None:
    cmd = get_search_cmd(clp_home, clp_config_path, dataset)
    proc = subprocess.run(cmd, check=True, capture_output=True)
    search_results = proc.stdout.decode("utf-8")
    search_cnt = len(search_results.split("\n")) - 1

    if search_cnt != expected_cnt:
        raise ValueError(f"search results count mismatch: {search_cnt} != {expected_cnt}")


def get_search_cmd(clp_home, clp_config_path: Path, dataset: Optional[str]) -> List[str]:
    cmd = [str(clp_home / "sbin/search.sh"), "--config", str(clp_config_path), "*"]
    if dataset is not None:
        cmd.extend(["--dataset", dataset])
    return cmd


def main(argv: List[str]) -> int:
    clp_home: Path = get_clp_home()
    # Just for early failure
    clp_repo_root = get_clp_repo_root()
    # Validate and load config file
    base_clp_config: CLPConfig
    try:
        base_clp_config = load_config_file(clp_home)
    except Exception:
        logger.exception("Failed to load config.")
        return -1
    base_clp_config.make_config_paths_absolute(clp_home)

    for storage_engine in [StorageEngine.CLP_S, StorageEngine.CLP]:
        base_clp_config.package.storage_engine = str(storage_engine)
        base_clp_config.package.query_engine = str(storage_engine)
        test_basic_functionality(clp_home, base_clp_config)
        test_fault_tolerance(clp_home, base_clp_config)
        test_race_condition(clp_home, base_clp_config)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
