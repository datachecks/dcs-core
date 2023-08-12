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
from typing import List

from loguru import logger

from datachecks.core.configuration.configuration import MetricConfiguration
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.logger.base import MetricLogger
from datachecks.core.metric.numeric_metric import *
from datachecks.core.metric.reliability_metric import *


class MetricManager:
    METRIC_CLASS_MAPPING = {
        MetricsType.DOCUMENT_COUNT.value: "DocumentCountMetric",
        MetricsType.ROW_COUNT.value: "RowCountMetric",
        MetricsType.MAX.value: "MaxMetric",
        MetricsType.FRESHNESS.value: "FreshnessValueMetric",
    }

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
                params = {
                    "filters": asdict(metric_config.filters)
                    if metric_config.filters
                    else None,
                }
                if metric_config.index:
                    params["index_name"] = metric_config.index
                if metric_config.table:
                    params["table_name"] = metric_config.table
                if metric_config.field:
                    params["field_name"] = metric_config.field

                logger.info(f"==============metric_config: {self.METRIC_CLASS_MAPPING}")
                logger.info(
                    f"==============metric_config.metric_type: {metric_config.metric_type}"
                )
                # logger.info(globals())
                metric: Metric = globals()[
                    self.METRIC_CLASS_MAPPING[metric_config.metric_type]
                ](
                    metric_config.name,
                    self.data_source_manager.get_data_source(data_source),
                    MetricsType(metric_config.metric_type.lower()),
                    self.metric_logger,
                    **params,
                )

                logger.info(metric.__dict__)
                self.metrics[metric.get_metric_identity()] = metric

    @property
    def get_metrics(self):
        return self.metrics

    def get_metric(self, metric_identity: str):
        return self.metrics.get(metric_identity, None)
