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

from dataclasses import asdict
from typing import Dict, List

from datachecks.core.configuration.configuration import MetricConfiguration
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.logger.base import MetricLogger
from datachecks.core.metric.base import Metric, MetricsType
from datachecks.core.metric.freshness_metric import FreshnessValueMetric
from datachecks.core.metric.numeric_metric import (DocumentCountMetric,
                                                   MaxMetric, RowCountMetric)


class MetricManager:
    def __init__(
        self,
        metric_config: Dict[str, List[MetricConfiguration]],
        data_source_manager: DataSourceManager,
        metric_logger: MetricLogger = None,
    ):
        self.data_source_manager = data_source_manager
        self.metrics: Dict[str, Metric] = {}
        self.metric_logger: MetricLogger = metric_logger
        self._build_metrics(config=metric_config)

    def _build_metrics(self, config: Dict[str, List[MetricConfiguration]]):
        for data_source, metric_list in config.items():
            for metric_config in metric_list:
                if metric_config.metric_type == MetricsType.DOCUMENT_COUNT:
                    metric = DocumentCountMetric(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(
                            data_source
                        ),
                        filters=asdict(metric_config.filters)
                        if metric_config.filters
                        else None,
                        index_name=metric_config.index,
                        metric_type=MetricsType.DOCUMENT_COUNT,
                        metric_logger=self.metric_logger,
                    )
                    self.metrics[metric.get_metric_identity()] = metric
                elif metric_config.metric_type == MetricsType.ROW_COUNT:
                    metric = RowCountMetric(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(
                            data_source
                        ),
                        filters=asdict(metric_config.filters)
                        if metric_config.filters
                        else None,
                        table_name=metric_config.table,
                        metric_type=MetricsType.ROW_COUNT,
                        metric_logger=self.metric_logger,
                    )
                    self.metrics[metric.get_metric_identity()] = metric
                elif metric_config.metric_type == MetricsType.MAX:
                    metric = MaxMetric(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(
                            data_source
                        ),
                        filters=asdict(metric_config.filters)
                        if metric_config.filters
                        else None,
                        table_name=metric_config.table,
                        index_name=metric_config.index,
                        metric_type=MetricsType.MAX,
                        field_name=metric_config.field,
                        metric_logger=self.metric_logger,
                    )
                    self.metrics[metric.get_metric_identity()] = metric
                elif metric_config.metric_type == MetricsType.FRESHNESS:
                    metric = FreshnessValueMetric(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(
                            data_source
                        ),
                        filters=asdict(metric_config.filters)
                        if metric_config.filters
                        else None,
                        table_name=metric_config.table,
                        index_name=metric_config.index,
                        metric_type=MetricsType.FRESHNESS,
                        field_name=metric_config.field,
                        metric_logger=self.metric_logger,
                    )
                    self.metrics[metric.get_metric_identity()] = metric
                else:
                    raise ValueError("Invalid metric type")

    @property
    def get_metrics(self):
        return self.metrics

    def get_metric(self, metric_identity: str):
        return self.metrics.get(metric_identity, None)
