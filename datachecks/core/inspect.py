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
from typing import List, Dict

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
        metric_values: List[Dict] = []
        for metric_name, metric in self.metric_manager.metrics.items():
            metric_value = metric.get_value()
            metric_values.append(metric_value)
        return metric_values
