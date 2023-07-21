from dataclasses import asdict
from typing import Dict, List

from datachecks.core.configuration.configuration import MetricConfiguration
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.metric.base import MetricsType, DocumentCountMetrics, RowCountMetrics


class MetricManager:

    def __init__(
            self,
            metric_config: Dict[str, List[MetricConfiguration]],
            data_source_manager: DataSourceManager
    ):
        self.data_source_manager = data_source_manager
        self.metrics = {}
        self._build_metrics(config=metric_config)

    def _build_metrics(self, config: Dict[str, List[MetricConfiguration]]):
        for data_source, metric_list in config.items():
            for metric_config in metric_list:
                if metric_config.metric_type == MetricsType.DOCUMENT_COUNT:

                    metric = DocumentCountMetrics(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(data_source),
                        filter=asdict(metric_config.filter),
                        index_name=metric_config.index
                    )
                    self.metrics[metric.metric_identity] = metric
                elif metric_config.metric_type == MetricsType.ROW_COUNT:
                    metric = RowCountMetrics(
                        name=metric_config.name,
                        data_source=self.data_source_manager.get_data_source(data_source),
                        filter=asdict(metric_config.filter),
                        table_name=metric_config.table
                    )
                    self.metrics[metric.metric_identity] = metric
                else:
                    raise ValueError("Invalid metric type")

    @property
    def get_metrics(self):
        return self.metrics

    def get_metric(self, metric_identity: str):
        return self.metrics.get(metric_identity, None)
