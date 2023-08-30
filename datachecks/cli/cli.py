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
import os
import sys
import traceback
import warnings
from typing import Union

import click
from loguru import logger
from rich import print
from rich.table import Table

from datachecks.__version__ import __version__
from datachecks.core import Configuration, Inspect, load_configuration
from datachecks.core.common.models.metric import DataSourceMetrics
from datachecks.core.inspect import InspectOutput

logger.remove()
logger.add(sys.stderr, level="WARNING")
warnings.filterwarnings("ignore")


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
    config_path: Union[str, None],
    auto_profile: bool = False,
):
    """
    Starts the datachecks inspection
    """
    try:
        is_file_exists = os.path.isfile(config_path)
        if not is_file_exists:
            raise Exception(
                f"Invalid value for '-C' / '--config-path': File '{config_path}' does not exist."
            )
        configuration: Configuration = load_configuration(config_path)

        inspector = Inspect(configuration=configuration, auto_profile=auto_profile)

        print("Starting [bold blue]datachecks[/bold blue] inspection...", ":zap:")
        output: InspectOutput = inspector.run()

        print("[bold green]Inspection completed successfully![/bold green] :tada:")
        print(f"Inspection took {inspector.execution_time_taken} seconds")

        print(_build_metric_cli_table(output))
        sys.exit(0)

    except Exception as e:
        print(f"[bold red]Failed to run datachecks inspection: {str(e)} [/bold red]")
        sys.exit(1)


def _build_metric_cli_table(inspect_output: InspectOutput):
    table = Table(
        title="List of Generated Metrics", show_header=True, header_style="bold blue"
    )
    table.add_column("Data Source", justify="right", style="cyan", no_wrap=True)
    table.add_column("Metric Type", style="magenta")
    table.add_column("Metric Identifier", style="magenta")
    table.add_column("Value", justify="right", style="green")
    for data_source_name, ds_metrics in inspect_output.metrics.items():
        if isinstance(ds_metrics, DataSourceMetrics):
            for tabel_name, table_metrics in ds_metrics.table_metrics.items():
                for metric_identifier, metric in table_metrics.metrics.items():
                    table.add_row(
                        f"{data_source_name}",
                        f"{metric.metric_type}",
                        f"{metric_identifier}",
                        f"{metric.value}",
                    )
            for index_name, index_metrics in ds_metrics.index_metrics.items():
                for metric_identifier, metric in index_metrics.metrics.items():
                    table.add_row(
                        f"{data_source_name}",
                        f"{metric.metric_type}",
                        f"{metric_identifier}",
                        f"{metric.value}",
                    )
        else:
            for metric_identifier, metric in ds_metrics.metrics.items():
                table.add_row(
                    f"",
                    f"{metric.metric_type}",
                    f"{metric_identifier}",
                    f"{metric.value}",
                )

    return table
