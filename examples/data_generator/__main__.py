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
from dotenv import load_dotenv

from examples.data_generator.datasets import DataSetName
from examples.data_generator.datasource import (
    BigQueryDataSource,
    DataBricksDataSource,
    DB2DataSource,
    ElasticSearchDataSource,
    MySqlDataSource,
    OpenSearchDataSource,
    PGSqlDataSource,
    RedShiftDataSource,
)

load_dotenv()


@click.command()
@click.option(
    "-d",
    "--datasource",
    type=click.Choice(
        [
            "pgsql",
            "databricks",
            "bigquery",
            "opensearch",
            "elasticsearch",
            "mysql",
            "redshift",
            "db2",
        ]
    ),
    help="Data source name",
)
@click.option(
    "-ds",
    "--dataset",
    type=click.Choice(DataSetName.__members__.values()),
    help="The dataset name",
)
def execute(datasource, dataset):
    dataset_name = DataSetName(dataset)

    if datasource == "pgsql":
        data_source = PGSqlDataSource()
    elif datasource == "mysql":
        data_source = MySqlDataSource()
    elif datasource == "databricks":
        data_source = DataBricksDataSource()
    elif datasource == "bigquery":
        data_source = BigQueryDataSource()
    elif datasource == "opensearch":
        data_source = OpenSearchDataSource()
    elif datasource == "elasticsearch":
        data_source = ElasticSearchDataSource()
    elif datasource == "redshift":
        data_source = RedShiftDataSource()
    elif datasource == "db2":
        data_source = DB2DataSource()
    else:
        raise Exception("Invalid datasource name")

    data_source.load_dataset(dataset_name)


if __name__ == "__main__":
    execute()
