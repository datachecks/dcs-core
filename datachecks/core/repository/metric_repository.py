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

import abc
from datetime import datetime
from typing import Dict, List, Tuple

from datachecks.core.common.models.metric import MetricValue


class MetricRepository(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def save_metric(self, metric_id, metric_value: MetricValue) -> int:
        """
        This method will save the metric_value for the given metric_id. The metric_value will be stored in two time
        granularity - daily and hourly.
        - daily: the time store value will be the start of the day. For example a metric value for 2020-01-01 10:30am
            will be stored as 2020-01-01 00:00:00. This is to ensure that we can query the metric value for a given day.
            If a metric is generated multiple times a day then the latest value will be stored and overwritten.
        - hourly: the time store value will be the start of the hour. For example a metric value for 2020-01-01 10:30am
            will be stored as 2020-01-01 10:00:00. This is to ensure that we can query the metric value for a given hour.
            If a metric is generated multiple times an hour then the latest value will be stored and overwritten.
        """
        pass

    @abc.abstractmethod
    def save_all_metrics(self, metrics: List[MetricValue]) -> int:
        """
        This method will save all the metrics in the given list. The metric_value will be stored in two time
        granularity - daily and hourly.
        """
        pass

    @abc.abstractmethod
    def get_metric_by_id(
        self, metric_id, start_date: datetime = None, end_date: datetime = None
    ) -> Tuple[List[MetricValue], List[MetricValue]]:
        """
        This method will return the metrics stored in the repository for the given metric_id. The metrics will be
        returned as a tuple of two lists. The first list will contain the daily metrics and the second list will contain
        the hourly metrics.

        return value:
        (
            [daily_metric_1, daily_metric_2],
            [hourly_metric_1, hourly_metric_2]
        )
        """
        pass

    @abc.abstractmethod
    def get_all_metrics(
        self, start_date: datetime = None, end_date: datetime = None
    ) -> Dict[str, Tuple[List[MetricValue], List[MetricValue]]]:
        """
        This method will return all the metrics stored in the repository. The metrics will be returned as a dictionary
        with the key being the metric_id and the value being a tuple of two lists. The first list will contain the
        daily metrics and the second list will contain the hourly metrics.

        return value:
        {
            "metric_id_1": ([daily_metric_1, daily_metric_2], [hourly_metric_1, hourly_metric_2]),
            "metric_id_2": ([daily_metric_1, daily_metric_2], [hourly_metric_1, hourly_metric_2])
        }
        """
        pass
