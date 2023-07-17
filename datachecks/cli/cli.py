import click
import yaml
from yaml import SafeLoader

from ..__version__ import __version__
from ..core.configuration.configuration import load_configuration, Configuration
from ..core.datasource.data_source import DataSourceManager


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
    for a in data_source_manager.get_data_source_names():
        d = data_source_manager.get_data_source(a)
        print(d.data_source_name)
        print(d.is_connected())

    """
    1. Read config
    2. Create data sources
    3. Create metrics
    4. for each metric, run it call get_value()
    """
