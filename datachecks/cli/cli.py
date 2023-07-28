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

from datachecks import Inspect
from datachecks.__version__ import __version__
from datachecks.core.configuration.configuration import (Configuration,
                                                         load_configuration)


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
    "-A",
    "--application-name",
    required=False,
    default="datachecks",
    help="Specify the application name for logging",
)
@click.option(
    "--time-format",
    required=False,
    default=None,
    help="Specify the time format for logging",
)
def inspect(
    config_path: Union[str, None] = None,
    application_name: str = "datachecks",
    time_format: str = None,
):
    """
    Starts the datachecks inspection
    """
    configuration: Configuration = load_configuration(config_path)

    configuration.metric_logger.config["application_name"] = application_name
    if time_format is not None:
        configuration.metric_logger.config["time_format"] = time_format

    inspector = Inspect(configuration=configuration)

    logger.info("Starting datachecks inspection...")
    inspector.start()
