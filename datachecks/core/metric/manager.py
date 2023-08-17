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

from loguru import logger

from datachecks.core.common.models.configuration import MetricConfiguration
from datachecks.core.common.models.data_source_resource import Field, Index, Table
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.metric.base import Metric
from datachecks.core.metric.numeric_metric import (  # noqa F401 this is used in globals
    AvgMetric,
    MaxMetric,
    MinMetric,
)
from datachecks.core.metric.reliability_metric import (  # noqa
    DocumentCountMetric,
    FreshnessValueMetric,
    RowCountMetric,
)


class MetricManager:
    METRIC_CLASS_MAPPING = {
        MetricsType.DOCUMENT_COUNT.value: "DocumentCountMetric",
        MetricsType.ROW_COUNT.value: "RowCountMetric",
        MetricsType.FRESHNESS.value: "FreshnessValueMetric",
        MetricsType.MAX.value: "MaxMetric",
        MetricsType.MIN.value: "MinMetric",
        MetricsType.AVG.value: "AvgMetric",
    }

    def __init__(
        self,
        metric_config: Dict[str, MetricConfiguration],
        data_source_manager: DataSourceManager,
    ):
        self.data_source_manager = data_source_manager
        self.metrics: Dict[str, Metric] = {}
        self._build_metrics(config=metric_config)

    def _build_metrics(self, config: Dict[str, MetricConfiguration]):
        for metric_name, metric_config in config.items():
            if isinstance(metric_config.resource, Field):
                data_source = metric_config.resource.belongs_to.data_source
            else:
                data_source = metric_config.resource.data_source
            params = {
                "filters": asdict(metric_config.filters)
                if metric_config.filters
                else None,
            }
            if isinstance(metric_config.resource, Index):
                params["index_name"] = metric_config.resource.name
            if isinstance(metric_config.resource, Table):
                params["table_name"] = metric_config.resource.name
            if isinstance(metric_config.resource, Field):
                params["field_name"] = metric_config.resource.name
                if isinstance(metric_config.resource.belongs_to, Table):
                    params["table_name"] = metric_config.resource.belongs_to.name
                elif isinstance(metric_config.resource.belongs_to, Index):
                    params["index_name"] = metric_config.resource.belongs_to.name

            metric: Metric = globals()[
                self.METRIC_CLASS_MAPPING[metric_config.metric_type]
            ](
                metric_config.name,
                self.data_source_manager.get_data_source(data_source),
                MetricsType(metric_config.metric_type.lower()),
                **params,
            )

            self.metrics[metric.get_metric_identity()] = metric

    def _build_metrics1(self, config: Dict[str, List[MetricConfiguration]]):
        for data_source, metric_list in config.items():
            for metric_config in metric_list:
                params = {
                    "filters": asdict(metric_config.filters)
                    if metric_config.filters
                    else None,
                }
                if isinstance(metric_config.resource, Index):
                    params["index_name"] = metric_config.resource.name
                if isinstance(metric_config.resource, Table):
                    params["table_name"] = metric_config.resource.name
                if isinstance(metric_config.resource, Field):
                    params["field_name"] = metric_config.resource.name
                    if isinstance(metric_config.resource.belongs_to, Table):
                        params["table_name"] = metric_config.resource.belongs_to.name
                    elif isinstance(metric_config.resource.belongs_to, Index):
                        params["index_name"] = metric_config.resource.belongs_to.name

                metric: Metric = globals()[
                    self.METRIC_CLASS_MAPPING[metric_config.metric_type]
                ](
                    metric_config.name,
                    self.data_source_manager.get_data_source(data_source),
                    MetricsType(metric_config.metric_type.lower()),
                    **params,
                )

                self.metrics[metric.get_metric_identity()] = metric

    @property
    def get_metrics(self):
        return self.metrics

    def get_metric(self, metric_identity: str):
        return self.metrics.get(metric_identity, None)
