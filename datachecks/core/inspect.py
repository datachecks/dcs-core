from datachecks.core.configuration.configuration import Configuration
from datachecks.core.datasource.data_source import DataSourceManager
from datachecks.core.metric.metric import MetricManager


class Inspect:

    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self.data_source_manager = DataSourceManager(configuration.data_sources)
        self.metric_manager = MetricManager(
            metric_config=configuration.metrics,
            data_source_manager=self.data_source_manager
        )

    def start(self):
        for metric_name, metric in self.metric_manager.metrics.items():
            metric_value = metric.get_value()
            print(f"{metric_name} : {metric_value}")
