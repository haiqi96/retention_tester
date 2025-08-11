from __future__ import annotations

import shutil
import subprocess
from contextlib import closing
from copy import deepcopy
from dataclasses import dataclass
import os
import sys
from typing import List, Optional, Final

import yaml

from clp_package_utils.general import get_clp_home, \
    CLP_DEFAULT_CONFIG_FILE_RELATIVE_PATH, validate_and_load_db_credentials_file
from clp_py_utils.clp_config import CLPConfig, Database, ArchiveOutput, StorageType
from pathlib import Path
from datetime import datetime
import json
import logging
from clp_py_utils.clp_metadata_db_utils import fetch_existing_datasets, get_archives_table_name
from clp_py_utils.clp_config import CLP_DEFAULT_DATASET_NAME
from clp_py_utils.core import read_yaml_config_file
from clp_py_utils.sql_adapter import SQL_Adapter
from job_orchestration.garbage_collector.constants import SECOND_TO_MILLISECOND

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


def load_config_file(
    clp_home: Path
):
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

def start_clp(clp_home: Path, retention_test_config: Path):
    cmd = [str(clp_home / "sbin/start-clp.sh"), "--config", str(retention_test_config)]
    subprocess.run(
        cmd,
        cwd=clp_home,
        check=True
    )


def clear_clp_package(clp_config: CLPConfig):
    if clp_config.logs_directory.exists():
        shutil.rmtree(clp_config.logs_directory)
    if clp_config.data_directory.exists():
        shutil.rmtree(clp_config.data_directory)

def execute_compression(clp_home: Path, input_log: Path, timestamp_key: Optional[str] = None):
    cmd = [str(clp_home / "sbin/compress.sh"), str(input_log)]
    if timestamp_key is not None:
        cmd.extend(["--timestamp-key", timestamp_key])
    subprocess.run(
        cmd,
        check=True
    )


def stop_clp(clp_home: Path):
    cmd = [str(clp_home / "sbin/stop-clp.sh"), "-f"]
    subprocess.run(
        cmd,
        check=True
    )


def find_archives_in_metadata_database(database_config: Database, dataset: Optional[str] = None):
    sql_adapter: SQL_Adapter = SQL_Adapter(database_config)
    clp_db_connection_params: dict[str, any] = (
        database_config.get_clp_connection_params_and_type(True)
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
        archives_list = [item['archive_id'] for item in db_cursor.fetchall()]
        return archives_list


def find_archives_on_disk(archive_output_config: ArchiveOutput, dataset: Optional[str] = None) -> List[str]:
    storage_type = archive_output_config.storage.type
    if StorageType.FS != archive_output_config.storage.type:
        raise ValueError(f"Unexpected storage type {storage_type}")

    archive_directory = archive_output_config.get_directory()
    if dataset is not None:
        archive_directory = archive_directory / dataset

    archives_list = [item.name for item in archive_directory.iterdir()]
    return archives_list


def configure_retention(base_clp_config: CLPConfig, archive_config: Optional[RetentionConfig] = None, search_result_config: Optional[RetentionConfig] = None) -> CLPConfig:
    retention_test_config = deepcopy(base_clp_config)

    garbage_collector_config = retention_test_config.garbage_collector
    garbage_collector_config.logging_level = "DEBUG"

    if archive_config is not None:
        retention_test_config.archive_output.retention_period = archive_config.retention_period_minutes
        garbage_collector_config.sweep_interval.archive = archive_config.sweep_interval_minutes

    if search_result_config is not None:
        retention_test_config.results_cache.retention_period = search_result_config.retention_period_minutes
        garbage_collector_config.sweep_interval.search_result = search_result_config.sweep_interval_minutes

    return retention_test_config


def get_formatted_ts(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime(TS_FORMAT_STRING)[:-3]

def generate_unstructured_log(log_path: Path, reference_sec_epoch: Optional[float]) -> None:
    log_ts = reference_sec_epoch
    log_template = "This is an example log message with id={}\n"

    with open(log_path, 'w') as output:
        for i in range(100):
            log_msg = log_template.format(i)
            if log_ts is not None:
                log_ts += LOG_TS_DELTA
                log_msg = f"{get_formatted_ts(log_ts)} {log_msg}"
            output.write(log_msg)


def generate_structured_log(log_path: Path, reference_sec_epoch: Optional[float]) -> None:
    log_ts = reference_sec_epoch
    json_log_template = {
        "msg": "Example log message",
    }

    with open(log_path, 'w') as output:
        for i in range(100):
            json_log_template["id"] = i
            if log_ts is not None:
                log_ts += LOG_TS_DELTA
                json_log_template["timestamp"] = get_formatted_ts(log_ts)
            output.write(json.dumps(json_log_template, indent=None))
            output.write("\n")


def main(argv: List[str]) -> int:
    package_path = argv[0]

    clp_home = get_clp_home()
    # Validate and load config file
    base_clp_config: CLPConfig
    try:
        base_clp_config = load_config_file(clp_home)
    except Exception:
        logger.exception("Failed to load config.")
        return -1

    test_retention_minutes = 8
    test_sweep_interval_minutes = 4

    archive_retention = RetentionConfig(test_retention_minutes, test_sweep_interval_minutes)
    retention_test_config = configure_retention(base_clp_config, archive_config=archive_retention)
    retention_test_config_path = clp_home / "etc" / "retention-test-config.yml"
    with open(retention_test_config_path, "w") as f:
        yaml.safe_dump(retention_test_config.dump_to_primitive_dict(), f)

    start_clp(clp_home, retention_test_config_path)
    # text_log_path = Path("test.log").resolve()
    # execute_compression(clp_home, text_log_path)

    json_log_path = Path("test.jsonl").resolve()
    execute_compression(clp_home, json_log_path, "timestamp")

    retention_test_config.make_config_paths_absolute(clp_home)
    validate_and_load_db_credentials_file(retention_test_config, clp_home, True)
    archives = find_archives_in_metadata_database(retention_test_config.database, CLP_DEFAULT_DATASET_NAME)
    print(archives)
    print(find_archives_on_disk(retention_test_config.archive_output, CLP_DEFAULT_DATASET_NAME))
    stop_clp(clp_home)
    clear_clp_package(retention_test_config)
    # text_log_path = Path("test.log")
    # json_log_path = Path("test.jsonl")
    # test_begin_ts = datetime.now().timestamp()
    # generate_unstructured_log(text_log_path, test_begin_ts)
    # generate_structured_log(json_log_path, test_begin_ts)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))