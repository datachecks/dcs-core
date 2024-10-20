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
import uuid
import warnings
from typing import Union

import click
from loguru import logger
from rich import print
from rich.table import Table, Text

from dcs_core.__version__ import __version__
from dcs_core.core import Configuration, Inspect
from dcs_core.core.configuration.configuration_parser import load_configuration

# from datachecks.core.common.models.metric import DataSourceMetrics
from dcs_core.core.inspect import InspectOutput
from dcs_core.report.dashboard import DashboardInfoBuilder, html_template
from dcs_core.report.models import TemplateParams

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
# Disabled for now TODO: Enable in future for validations
# @click.option(
#     "--auto-profile",
#     is_flag=True,
#     help="Specify if the inspection should do auto-profile of all data sources",
# )
# @click.option(
#     "--html-report",
#     is_flag=True,
#     help="Specify if the inspection should generate HTML report",
# )
# @click.option(
#     "--report-path",
#     required=False,
#     default="datachecks_report.html",
#     help="Specify the file path for HTML report",
# )
def inspect(
    config_path: Union[str, None],
    # auto_profile: bool = False, # Disabled for now
    # html_report: bool = False,
    # report_path: str = "datachecks_report.html",
):
    """
    Starts the datachecks inspection
    """
    try:
        is_file_exists = os.path.exists(config_path)
        if not is_file_exists:
            raise Exception(
                f"Invalid value for '-C' / '--config-path': File '{config_path}' does not exist."
            )
        configuration: Configuration = load_configuration(config_path)
        inspector = Inspect(configuration=configuration)

        print("Starting [bold blue]datachecks[/bold blue] inspection...", ":zap:")
        output: InspectOutput = inspector.run()

        print("[bold green]Inspection completed successfully![/bold green] :tada:")
        print(f"Inspection took {inspector.execution_time_taken} seconds")
        # Disable for now
        # if html_report:
        #     print(f"Generating HTML report at {report_path}")
        #     _build_html_report(inspect_output=output, report_path=report_path)
        #     print(f"HTML report generated at {report_path}")
        # else:
        print(_build_metric_cli_table(inspect_output=output))
        sys.exit(0)

    except Exception as e:
        print(f"[bold red]Failed to run datachecks inspection: {str(e)} [/bold red]")
        sys.exit(1)


def _build_metric_cli_table(*, inspect_output: InspectOutput):
    table = Table(
        title="List of Validations",
        show_header=True,
        header_style="bold blue",
    )
    table.add_column("Validation Name", style="cyan", no_wrap=True)
    table.add_column("Data Source", style="magenta")
    table.add_column("Validation Type", style="magenta")
    table.add_column("Value", justify="right", style="green")
    table.add_column("Is Valid", justify="right")
    table.add_column("Reason", justify="right")

    for identy, validation_info in inspect_output.validations.items():
        _validity_style = (
            ""
            if validation_info.is_valid is None
            else "red"
            if not validation_info.is_valid
            else "green"
        )
        value = (
            validation_info.name,
            validation_info.data_source_name,
            validation_info.validation_function,
            str(validation_info.value),
            Text(
                "-"
                if validation_info.is_valid is None
                else "Failed"
                if not validation_info.is_valid
                else "Passed",
                style=_validity_style,
            ),
            "-" if validation_info.reason is None else validation_info.reason,
        )
        table.add_row(*value)

    # for data_source_name, ds_metrics in inspect_output.metrics.items():
    #     row = None
    #     if isinstance(ds_metrics, DataSourceMetrics):
    #         for tabel_name, table_metrics in ds_metrics.table_metrics.items():
    #             for metric_identifier, metric in table_metrics.metrics.items():
    #                 table.add_row(
    #                     *_build_row(metric),
    #                 )
    #         for index_name, index_metrics in ds_metrics.index_metrics.items():
    #             for metric_identifier, metric in index_metrics.metrics.items():
    #                 table.add_row(
    #                     *_build_row(metric),
    #                 )
    #     else:
    #         for metric_identifier, metric in ds_metrics.metrics.items():
    #             table.add_row(
    #                 *_build_row(metric),
    #             )

    return table


def _build_html_report(*, inspect_output: InspectOutput, report_path: str):
    template_params = TemplateParams(
        dashboard_id="dcs_dashboard_" + str(uuid.uuid4()).replace("-", ""),
        dashboard_info=DashboardInfoBuilder(inspect_output).build(),
    )

    with open(report_path, "w", encoding="utf-8") as out_file:
        out_file.write(html_template(template_params))
