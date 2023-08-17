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
from typing import Union

import click
from loguru import logger

from datachecks.__version__ import __version__
from datachecks.core import Configuration, Inspect, load_configuration


@click.version_option(package_name="datachecks", prog_name="datachecks")
@click.group(help=f"Datachecks CLI version {__version__}")
def main():
    pass


@main.command(
    short_help="Starts the datachecks inspection",
)
@click.option(
    "-C",
    "--config-path",
    required=True,
    default=None,
    help="Specify the file path for configuration",
)
@click.option(
    "--auto-profile",
    is_flag=True,
    help="Specify if the inspection should do auto-profile of all data sources",
)
def inspect(
    config_path: Union[str, None] = None,
    auto_profile: bool = False,
):
    """
    Starts the datachecks inspection
    """
    configuration: Configuration = load_configuration(config_path)

    inspector = Inspect(configuration=configuration, auto_profile=auto_profile)

    logger.info("Starting datachecks inspection...")
    output = inspector.run()

    for ds_name, ds_met in output.metrics.items():
        logger.info(f"==================={ds_name}==================")
        for tabel_name, table_met in ds_met.table_metrics.items():
            logger.info(f"-----------------{tabel_name}-----------------")
            for met_name, met in table_met.metrics.items():
                logger.info(f"{met_name}: {met.value}")
        for index_name, index_met in ds_met.index_metrics.items():
            logger.info(f"-----------------{index_name}-----------------")
            for met_name, met in index_met.metrics.items():
                logger.info(f"{met_name}: {met.value}")
