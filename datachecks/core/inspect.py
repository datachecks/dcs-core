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
from typing import Dict, List

import requests

from datachecks.core.configuration.configuration import Configuration
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.logger.default_logger import DefaultLogger
from datachecks.core.metric.manager import MetricManager

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


class Inspect:
    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self.data_source_manager = DataSourceManager(configuration.data_sources)

        self.metric_logger = None

        if self.configuration.metric_logger:
            if self.configuration.metric_logger.type == "default":
                self.metric_logger = DefaultLogger(
                    **self.configuration.metric_logger.config
                    if self.configuration.metric_logger.config
                    else {}
                )

        self.metric_manager = MetricManager(
            metric_config=configuration.metrics,
            data_source_manager=self.data_source_manager,
            metric_logger=self.metric_logger,
        )

    def start(self):
        metric_values: List[Dict] = []
        for metric_name, metric in self.metric_manager.metrics.items():
            metric_value = metric.get_value()
            metric_values.append(metric_value)
        return metric_values
