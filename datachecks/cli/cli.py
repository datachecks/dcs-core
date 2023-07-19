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

import click
import yaml
from yaml import SafeLoader

from ..__version__ import __version__
from ..core.configuration.configuration import load_configuration, Configuration
from ..core.datasource.data_source import DataSourceManager
from ..core.metric.metric import MetricManager


@click.version_option(package_name="datachecks", prog_name="datachecks")
@click.group(help=f"Datachecks CLI version {__version__}")
def main():
    pass


@main.command(
    short_help="Starts the datachecks inspection",
)
def inspect():
    configuration: Configuration = load_configuration('config.yaml')

    data_source_manager = DataSourceManager(configuration.data_sources)
    for data_source_name in data_source_manager.get_data_source_names():
        data_source = data_source_manager.get_data_source(data_source_name=data_source_name)
        print(f"Data source: {data_source.data_source_name} is {data_source.is_connected()}")
    metric_manager = MetricManager(
        metric_config=configuration.metrics,
        data_source_manager=data_source_manager
    )
    for metric_name, metric in metric_manager.metrics.items():
        metric_value = metric.get_value()
        print(f"{metric_name} : {metric_value}")


    """
    1. Read config
    2. Create data sources
    3. Create metrics
    4. for each metric, run it call get_value()
    """
