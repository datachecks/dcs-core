#  Copyright 2022-present, the Waterdip Labs Pvt. Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import pytz

from datachecks.core.common.errors import DataChecksRuntimeError
from datachecks.core.common.models.metric import MetricsType, MetricValue
from datachecks.core.utils.utils import write_to_file
from datachecks.integrations.storage.local_file import LocalFileMetricRepository

INTEGRATION_TEST_DIR = "/tmp/datachecks/local_file_integration"

metrics = [
    (
        "iris.dcs_iris.row_count.postgres_row_count",
        MetricValue(
            identity="iris.dcs_iris.row_count.postgres_row_count",
            metric_type=MetricsType.ROW_COUNT,
            value=10,
            timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
        ),
    ),
    (
        "iris.dcs_iris.row_count.postgres_row_count1",
        MetricValue(
            identity="iris.dcs_iris.row_count.postgres_row_count1",
            metric_type=MetricsType.ROW_COUNT,
            value=11,
            timestamp=datetime(2023, 11, 20, tzinfo=pytz.UTC),
        ),
    ),
    (
        "iris.dcs_iris.row_count.postgres_row_count1",
        MetricValue(
            identity="iris.dcs_iris.row_count.postgres_row_count1",
            metric_type=MetricsType.ROW_COUNT,
            value=12,
            timestamp=datetime(2023, 11, 20, 1, tzinfo=pytz.UTC),
        ),
    ),
]


def create_initial_metric(base_dir, metric_identity: str, mv: MetricValue):
    daily_directory = f"{base_dir}/{metric_identity}/daily"
    hourly_directory = f"{base_dir}/{metric_identity}/hourly"
    os.makedirs(daily_directory, exist_ok=True)
    os.makedirs(hourly_directory, exist_ok=True)
    (
        timestamp_date_key,
        timestamp_hour_key,
    ) = LocalFileMetricRepository.get_timestamp_keys(mv)
    daily_file_name = f"{daily_directory}/{timestamp_date_key}.json"
    hourly_file_name = f"{hourly_directory}/{timestamp_hour_key}.json"
    write_to_file(daily_file_name, mv.json)
    write_to_file(hourly_file_name, mv.json)


def create_initial_metrics():
    dir_name = f"{INTEGRATION_TEST_DIR}/initial_metrics/metrics"
    os.makedirs(dir_name)
    for metric in metrics:
        create_initial_metric(dir_name, metric[0], metric[1])


@pytest.fixture(scope="module", autouse=True)
def local_metric_storage_fixture():
    import shutil

    shutil.rmtree(INTEGRATION_TEST_DIR, ignore_errors=True)
    os.makedirs(INTEGRATION_TEST_DIR, exist_ok=True)
    create_initial_metrics()
    yield
    shutil.rmtree(INTEGRATION_TEST_DIR)


def test_should_read_metric_from_files():
    mr = LocalFileMetricRepository(f"{INTEGRATION_TEST_DIR}/initial_metrics")
    mv = mr.get_metric_by_id("iris.dcs_iris.row_count.postgres_row_count1")
    assert len(mv[0]) == 1
    assert len(mv[1]) == 2


def test_should_read_all_metrics_from_files():
    mr = LocalFileMetricRepository(f"{INTEGRATION_TEST_DIR}/initial_metrics")
    mv = mr.get_all_metrics()
    assert len(mv) == 2


def test_should_read_all_metrics_from_files_with_date_range():
    mr = LocalFileMetricRepository(f"{INTEGRATION_TEST_DIR}/initial_metrics")
    metric_dict = mr.get_all_metrics(
        start_date=datetime(2023, 11, 19, tzinfo=pytz.UTC),
        end_date=datetime(2023, 11, 20, tzinfo=pytz.UTC),
    )
    assert len(metric_dict.keys()) == 2
    row_count_metric = metric_dict["iris.dcs_iris.row_count.postgres_row_count"]
    row_count_metric1 = metric_dict["iris.dcs_iris.row_count.postgres_row_count1"]
    assert len(row_count_metric[0]) == 0
    assert len(row_count_metric[1]) == 0
    assert len(row_count_metric1[0]) == 1
    assert len(row_count_metric1[1]) == 2


def test_should_throw_error_when_both_start_and_end_date_are_not_provided():
    mr = LocalFileMetricRepository(f"{INTEGRATION_TEST_DIR}/initial_metrics")
    with pytest.raises(Exception):
        mr.get_all_metrics(start_date=datetime(2023, 11, 19, tzinfo=pytz.UTC))


def test_should_write_metrics():
    local_file_repository = LocalFileMetricRepository(
        f"{INTEGRATION_TEST_DIR}/test_metrics"
    )
    metric_identity = "iris.dcs_iris.row_count.postgres_row_count_write_test"
    local_file_repository.save_metric(
        metric_identity,
        MetricValue(
            identity=metric_identity,
            metric_type=MetricsType.ROW_COUNT,
            value=10,
            timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
            tags={"tag1": "tag1"},
        ),
    )
    daily_metrics = Path(
        f"{INTEGRATION_TEST_DIR}/test_metrics/metrics/{metric_identity}/daily/"
    )
    hourly_metrics = Path(
        f"{INTEGRATION_TEST_DIR}/test_metrics/metrics/{metric_identity}/hourly/"
    )
    assert daily_metrics.is_dir()
    assert hourly_metrics.exists()

    assert len(list(daily_metrics.glob("2023-11-21.json"))) == 1
    assert len(list(hourly_metrics.glob("2023-11-21_0.json"))) == 1


def test_should_save_all_metrics():
    test_metrics = [
        MetricValue(
            identity="iris.dcs_iris.row_count.postgres_row_count_write_all_test1",
            metric_type=MetricsType.ROW_COUNT,
            value=10,
            timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
            tags={"tag1": "tag1"},
        ),
        MetricValue(
            identity="iris.dcs_iris.row_count.postgres_row_count_write_all_test2",
            metric_type=MetricsType.ROW_COUNT,
            value=10,
            timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
            tags={"tag1": "tag1"},
        ),
    ]
    local_file_repository = LocalFileMetricRepository(
        f"{INTEGRATION_TEST_DIR}/test_metrics_all"
    )
    local_file_repository.save_all_metrics(test_metrics)

    test_metrics_get_all = local_file_repository.get_all_metrics()
    assert len(test_metrics_get_all.keys()) == 2
    assert (
        len(
            test_metrics_get_all[
                "iris.dcs_iris.row_count.postgres_row_count_write_all_test1"
            ][0]
        )
        == 1
    )
    assert (
        len(
            test_metrics_get_all[
                "iris.dcs_iris.row_count.postgres_row_count_write_all_test1"
            ][1]
        )
        == 1
    )
    assert (
        len(
            test_metrics_get_all[
                "iris.dcs_iris.row_count.postgres_row_count_write_all_test2"
            ][0]
        )
        == 1
    )
    assert (
        len(
            test_metrics_get_all[
                "iris.dcs_iris.row_count.postgres_row_count_write_all_test2"
            ][1]
        )
        == 1
    )


def test_should_throw_exception_if_base_dir_not_exists():
    with pytest.raises(DataChecksRuntimeError):
        LocalFileMetricRepository(f"/unknown/test_metrics_not_exists")


def test_should_throw_exception_if_not_able_to_save_metric():
    with patch(
        "datachecks.integrations.storage.local_file.write_to_file",
        side_effect=Exception("mocked error"),
    ):
        with pytest.raises(DataChecksRuntimeError):
            local_file_repository = LocalFileMetricRepository(
                f"{INTEGRATION_TEST_DIR}/test_metrics_exception_save"
            )
            local_file_repository.save_metric(
                "iris.dcs_iris.row_count.postgres_row_count_write_test",
                MetricValue(
                    identity="iris.dcs_iris.row_count.postgres_row_count_write_test",
                    metric_type=MetricsType.ROW_COUNT,
                    value=10,
                    timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
                    tags={"tag1": "tag1"},
                ),
            )


def test_should_throw_exception_if_not_able_to_save_all_metric():
    with patch(
        "datachecks.integrations.storage.local_file.write_to_file",
        side_effect=Exception("mocked error"),
    ):
        with pytest.raises(DataChecksRuntimeError):
            local_file_repository = LocalFileMetricRepository(
                f"{INTEGRATION_TEST_DIR}/test_metrics_exception_save"
            )
            local_file_repository.save_all_metrics(
                [
                    "iris.dcs_iris.row_count.postgres_row_count_write_test",
                    MetricValue(
                        identity="iris.dcs_iris.row_count.postgres_row_count_write_test",
                        metric_type=MetricsType.ROW_COUNT,
                        value=10,
                        timestamp=datetime(2023, 11, 21, tzinfo=pytz.UTC),
                        tags={"tag1": "tag1"},
                    ),
                ]
            )
