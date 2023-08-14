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

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration
from datachecks.core.datasource.opensearch import OpenSearchSearchIndexDataSource
from datachecks.core.datasource.postgres import PostgresSQLDatasource
from datachecks.core.metric.base import MetricsType
from datachecks.core.metric.reliability_metric import FreshnessValueMetric, RowCountMetric, DocumentCountMetric
from tests.utils import create_opensearch_client, create_postgres_connection

INDEX_NAME = "reliability_metric_test"
TABLE_NAME = "reliability_metric_test_table"


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


@pytest.mark.usefixtures("setup_data", "opensearch_datasource")
class TestDocumentCountMetric:
    def test_should_return_document_count_metric_without_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        doc = DocumentCountMetric(
            name="document_count_metric_test",
            data_source=opensearch_datasource,
            index_name=INDEX_NAME,
            metric_type=MetricsType.DOCUMENT_COUNT,
        )
        doc_value = doc.get_value()
        assert doc_value["value"] == 5

    def test_should_return_document_count_metric_with_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        doc = DocumentCountMetric(
            name="document_count_metric_test_1",
            data_source=opensearch_datasource,
            index_name=INDEX_NAME,
            metric_type=MetricsType.DOCUMENT_COUNT,
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 40}}}'},
        )
        doc_value = doc.get_value()
        assert doc_value["value"] == 2


@pytest.mark.usefixtures("setup_data", "postgres_datasource")
class TestRowCountMetric:
    def test_should_return_row_count_metric_without_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = RowCountMetric(
            name="row_count_metric_test",
            data_source=postgres_datasource,
            table_name=TABLE_NAME,
            metric_type=MetricsType.ROW_COUNT,
        )
        row_value = row.get_value()
        assert row_value["value"] == 5

    def test_should_return_row_count_metric_with_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = RowCountMetric(
            name="row_count_metric_test_1",
            data_source=postgres_datasource,
            table_name=TABLE_NAME,
            metric_type=MetricsType.ROW_COUNT,
            filters={"where_clause": "age >= 30 AND age <= 40"},
        )
        row_value = row.get_value()
        assert row_value["value"] == 2


@pytest.mark.usefixtures("setup_data", "postgres_datasource", "opensearch_datasource")
class TestFreshnessValueMetric:
    def test_should_get_freshness_value_from_opensearch(self, opensearch_datasource):
        metric = FreshnessValueMetric(
            name="freshness_value_metric_test",
            data_source=opensearch_datasource,
            index_name=INDEX_NAME,
            field_name="last_fight",
            metric_type=MetricsType.FRESHNESS,
        )
        metric_value = metric.get_value()
        assert metric_value["value"] == 3 * 3600 * 24

    def test_should_get_freshness_value_from_postgres(self, postgres_datasource):
        metric = FreshnessValueMetric(
            name="freshness_value_metric_test",
            data_source=postgres_datasource,
            table_name=TABLE_NAME,
            field_name="last_fight",
            metric_type=MetricsType.FRESHNESS,
        )
        metric_value = metric.get_value()
        assert metric_value["value"] >= 3 * 3600 * 24
