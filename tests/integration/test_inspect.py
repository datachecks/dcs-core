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
import datetime

import pytest
from opensearchpy import OpenSearch
from sqlalchemy import Connection, text

from datachecks.core.common.models.configuration import (
    DataSourceConfiguration,
    DataSourceConnectionConfiguration,
    DatasourceType,
    MetricConfiguration,
)
from datachecks.core.common.models.data_source_resource import Table
from datachecks.core.common.models.metric import MetricsType
from tests.utils import create_opensearch_client, create_postgres_connection

INDEX_NAME = "inspect_metric_test"
TABLE_NAME = "inspect_metric_test_table"


def populate_opensearch_datasource(opensearch_client: OpenSearch):
    try:
        opensearch_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
        opensearch_client.indices.create(
            index=INDEX_NAME,
            body={"mappings": {"properties": {"last_fight": {"type": "date"}}}},
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "thor",
                "age": 1500,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=10),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "captain america",
                "age": 100,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=3),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "iron man",
                "age": 50,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=4),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "hawk eye",
                "age": 40,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=5),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "black widow",
                "age": 35,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=6),
            },
        )
        opensearch_client.indices.refresh(index=INDEX_NAME)
    except Exception as e:
        print(e)


def populate_postgres_datasource(postgresql_connection: Connection):
    try:
        postgresql_connection.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (name VARCHAR(50), last_fight timestamp, age INTEGER)
        """
            )
        )
        postgresql_connection.commit()

        insert_query = f"""
            INSERT INTO {TABLE_NAME} VALUES
            ('thor', '{(datetime.datetime.utcnow() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")}', 1500),
            ('captain america', '{(datetime.datetime.utcnow() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")}', 90),
            ('iron man', '{(datetime.datetime.utcnow() - datetime.timedelta(days=4)).strftime("%Y-%m-%d")}', 50),
            ('hawk eye', '{(datetime.datetime.utcnow() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")}', 40),
            ('black widow', '{(datetime.datetime.utcnow() - datetime.timedelta(days=6)).strftime("%Y-%m-%d")}', 35)
        """

        postgresql_connection.execute(text(insert_query))
        postgresql_connection.commit()
    except Exception as e:
        print(e)


@pytest.fixture(scope="class")
@pytest.mark.usefixtures(
    "opensearch_client_configuration", "pgsql_connection_configuration"
)
def setup_data(
    opensearch_client_configuration: DataSourceConnectionConfiguration,
    pgsql_connection_configuration: DataSourceConnectionConfiguration,
):
    opensearch_client = create_opensearch_client(opensearch_client_configuration)
    postgresql_connection = create_postgres_connection(pgsql_connection_configuration)
    try:
        populate_opensearch_datasource(opensearch_client)
        populate_postgres_datasource(postgresql_connection)
        yield True
    except Exception as e:
        print(e)
    finally:
        opensearch_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
        opensearch_client.close()

        postgresql_connection.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
        postgresql_connection.commit()

        postgresql_connection.close()


@pytest.mark.usefixtures(
    "setup_data", "opensearch_client_configuration", "pgsql_connection_configuration"
)
class TestInspect:
    data_source_name = "test_open_search_data_source"

    @pytest.fixture(autouse=True)
    def _set_configuration(
        self, pgsql_connection_configuration: DataSourceConnectionConfiguration
    ):
        self.data_source_configuration = [
            DataSourceConfiguration(
                name=self.data_source_name,
                type=DatasourceType.POSTGRES,
                connection_config=DataSourceConnectionConfiguration(
                    host=pgsql_connection_configuration.host,
                    port=pgsql_connection_configuration.port,
                    database=pgsql_connection_configuration.database,
                    username=pgsql_connection_configuration.username,
                    password=pgsql_connection_configuration.password,
                ),
            )
        ]
        self.metrics = {
            "metric1": MetricConfiguration(
                name="metric1",
                metric_type=MetricsType.ROW_COUNT,
                resource=Table(
                    name=TABLE_NAME,
                    data_source=self.DATA_SOURCE_NAME,
                ),
            )
        }
