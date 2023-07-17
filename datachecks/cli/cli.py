import click
import yaml
from yaml import SafeLoader

from ..__version__ import __version__


@click.version_option(package_name="datachecks", prog_name="datachecks")
@click.group(help=f"Datachecks CLI version {__version__}")
def main():
    pass


@main.command(
    short_help="Runs a scan",
)
def inspect():
    print("====")
    with open('config.yaml') as f:
        data = yaml.load(f, Loader=SafeLoader)
        print(data)
    """
    1. Read config
    2. Create data sources
    3. Create metrics
    4. for each metric, run it call get_value()
    """
