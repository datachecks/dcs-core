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

import datetime as dt
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union

from datachecks.core.common.errors import DataChecksRuntimeError
from datachecks.core.common.models.metric import MetricValue
from datachecks.core.repository.metric_repository import MetricRepository
from datachecks.core.utils.utils import ensure_directory_exists, write_to_file


class LocalFileMetricRepository(MetricRepository):
    """
    Directory Structure:

    Dir:metric_identifier
    - Dir:daily:
        - file:metric_identifier__date1.json
        - file:metric_identifier__date2.json
    - Dir:hourly:
        - file:metric_identifier__date1.json
        - file:metric_identifier__date2.json
    """

    def __init__(self, storage_path):
        self.storage_path = f"{storage_path}/metrics"
        try:
            ensure_directory_exists(self.storage_path, create_if_not_exists=True)
        except Exception as e:
            raise DataChecksRuntimeError(
                f"Unable to locate metric storage directory: {self.storage_path} due to error: {e}"
            )

    @staticmethod
    def get_timestamp_keys(metric_value: MetricValue):
        metric_timestamp = metric_value.timestamp
        metric_timestamp_hour = metric_timestamp.hour

        # This is 'YYYY-MM-DD'.
        timestamp_date_key = metric_timestamp.date().isoformat()
        timestamp_hour_key = f"{timestamp_date_key}_{metric_timestamp_hour}"
        return timestamp_date_key, timestamp_hour_key

    def _generate_file_names(
        self, metric_id: str, metric_value: MetricValue
    ) -> Tuple[str, str]:
        timestamp_date_key, timestamp_hour_key = self.get_timestamp_keys(metric_value)

        daily_directory = f"{self.storage_path}/{metric_id}/daily"
        hourly_directory = f"{self.storage_path}/{metric_id}/hourly"

        ensure_directory_exists(daily_directory, create_if_not_exists=True)
        ensure_directory_exists(hourly_directory, create_if_not_exists=True)

        daily_file_name = f"{daily_directory}/{timestamp_date_key}.json"
        hourly_file_name = f"{hourly_directory}/{timestamp_hour_key}.json"

        return daily_file_name, hourly_file_name

    def save_metric(self, metric_id: str, metric_value: MetricValue) -> int:
        try:
            daily_file, hourly_file = self._generate_file_names(metric_id, metric_value)
            write_to_file(daily_file, metric_value.json)
            write_to_file(hourly_file, metric_value.json)
            return 1
        except Exception as e:
            raise DataChecksRuntimeError(
                f"Unable to save metric: {metric_id} due to error: {e}"
            )

    def save_all_metrics(self, metrics: List[MetricValue]) -> int:
        try:
            for metric in metrics:
                self.save_metric(metric.identity, metric)
            return len(metrics)
        except Exception as e:
            raise DataChecksRuntimeError(f"Unable to save metrics due to error: {e}")

    def _get_all_metric_files(
        self,
        metric_id: str,
        start_date: datetime = None,
        end_date: datetime = None,
        granularity: Literal["daily", "hourly"] = "daily",
    ) -> List[str]:
        directory_name = f"{self.storage_path}/{metric_id}/{granularity}"
        if start_date is None and end_date is None:
            return [directory_name + "/" + file for file in os.listdir(directory_name)]
        elif start_date is not None and end_date is not None:
            dates = [
                (start_date + dt.timedelta(days=x)).date().isoformat()
                for x in range((end_date - start_date).days + 1)
            ]

            if granularity == "hourly":
                return [
                    f"{directory_name}/{date}_{hour}.json"
                    for date in dates
                    for hour in range(0, 24)
                ]
            else:
                return [f"{directory_name}/{date}.json" for date in dates]
        else:
            raise DataChecksRuntimeError(
                "Both start_date and end_date should be provided or none of them should be provided."
            )

    @staticmethod
    def _json_to_metric_value(file_path: str) -> Union[MetricValue, None]:
        file_exists = Path(file_path).exists()
        if not file_exists:
            return None
        with open(file_path, "r") as f:
            json = f.read()
            return MetricValue.from_json(json)

    def get_metric_by_id(
        self, metric_id: str, start_date: datetime = None, end_date: datetime = None
    ) -> Tuple[List[MetricValue], List[MetricValue]]:
        daily_file_list = self._get_all_metric_files(
            metric_id, start_date, end_date, "daily"
        )
        hourly_file_list = self._get_all_metric_files(
            metric_id, start_date, end_date, "hourly"
        )
        daily_metrics: List[MetricValue] = []
        hourly_metrics: List[MetricValue] = []
        for file in daily_file_list:
            if (json_value := self._json_to_metric_value(file)) is not None:
                daily_metrics.append(json_value)
        for file in hourly_file_list:
            if (json_value := self._json_to_metric_value(file)) is not None:
                hourly_metrics.append(json_value)

        return daily_metrics, hourly_metrics

    def get_all_metrics(
        self, start_date: datetime = None, end_date: datetime = None
    ) -> Dict[str, Tuple[List[MetricValue], List[MetricValue]]]:
        metrics_ids = os.listdir(self.storage_path)

        metrics = {}
        for metric_id in metrics_ids:
            daily_metrics, hourly_metrics = self.get_metric_by_id(
                metric_id, start_date, end_date
            )
            metrics[metric_id] = (daily_metrics, hourly_metrics)

        return metrics
