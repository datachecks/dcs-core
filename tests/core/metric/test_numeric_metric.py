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

import pytest
from opensearchpy import OpenSearch
from sqlalchemy import Connection, text

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration
from datachecks.core.datasource.opensearch import \
    OpenSearchSearchIndexDataSource
from datachecks.core.datasource.postgres import PostgresSQLDatasource
from datachecks.core.metric.base import MetricsType
from datachecks.core.metric.numeric_metric import (DocumentCountMetric, MinMetric,
                                                   MaxMetric, AvgMetric, RowCountMetric)
from tests.utils import create_opensearch_client, create_postgres_connection


def populate_opensearch_datasource(opensearch_client: OpenSearch):
    try:
        opensearch_client.indices.delete(index="numeric_metric_test", ignore=[400, 404])
        opensearch_client.indices.create(index="numeric_metric_test")
        opensearch_client.index(
            index="numeric_metric_test", body={"name": "thor", "age": 501}
        )
        opensearch_client.index(
            index="numeric_metric_test", body={"name": "captain america", "age": 110}
        )
        opensearch_client.index(
            index="numeric_metric_test", body={"name": "iron man", "age": 35}
        )
        opensearch_client.index(
            index="numeric_metric_test", body={"name": "hawk eye", "age": 31}
        )
        opensearch_client.index(
            index="numeric_metric_test", body={"name": "black widow", "age": 30}
        )
        opensearch_client.indices.refresh(index="numeric_metric_test")
    except Exception as e:
        print(e)


def populate_postgres_datasource(postgresql_connection: Connection):
    try:
        postgresql_connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS numeric_metric_test (name VARCHAR(50), age INT)
        """
            )
        )
        postgresql_connection.execute(
            text(
                """
            INSERT INTO numeric_metric_test VALUES
            ('thor', 501), ('captain america', 110), ('iron man', 35), ('hawk eye', 31), ('black widow', 30)
        """
            )
        )
        postgresql_connection.commit()
    except Exception as e:
        print(e)


@pytest.fixture(scope="class", autouse=True)
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
        opensearch_client.indices.delete(index="numeric_metric_test", ignore=[400, 404])
        postgresql_connection.execute(text("DROP TABLE IF EXISTS numeric_metric_test"))
        postgresql_connection.commit()

        opensearch_client.close()
        postgresql_connection.close()


@pytest.mark.usefixtures("setup_data", "opensearch_datasource")
class TestDocumentCountMetric:
    def test_should_return_document_count_metric_without_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        doc = DocumentCountMetric(
            name="document_count_metric_test",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
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
            index_name="numeric_metric_test",
            metric_type=MetricsType.DOCUMENT_COUNT,
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 40}}}'},
        )
        doc_value = doc.get_value()
        assert doc_value["value"] == 3


@pytest.mark.usefixtures("setup_data", "postgres_datasource")
class TestRowCountMetric:
    def test_should_return_row_count_metric_without_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = RowCountMetric(
            name="row_count_metric_test",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
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
            table_name="numeric_metric_test",
            metric_type=MetricsType.ROW_COUNT,
            filters={"where_clause": "age >= 30 AND age <= 40"},
        )
        row_value = row.get_value()
        assert row_value["value"] == 3

@pytest.mark.usefixtures("setup_data", "postgres_datasource", "opensearch_datasource")
class TestMinColumnValueMetric:
    def test_should_return_min_column_value_postgres_without_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = MinMetric(
            name="min_metric_test",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_value()
        assert row_value["value"] == 30

    def test_should_return_min_column_value_postgres_with_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = MinMetric(
            name="min_metric_test_1",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"where_clause": "age >= 100 AND age <= 200"},
        )
        row_value = row.get_value()
        assert row_value["value"] == 110

    def test_should_return_min_column_value_opensearch_without_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = MinMetric(
            name="min_metric_test",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_value()
        assert row_value["value"] == 30

    def test_should_return_min_column_value_opensearch_with_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = MinMetric(
            name="min_metric_test_1",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 100, "lte": 200}}}'},
        )
        row_value = row.get_value()
        assert row_value["value"] == 110


@pytest.mark.usefixtures("setup_data", "postgres_datasource", "opensearch_datasource")
class TestMaxColumnValueMetric:
    def test_should_return_max_column_value_postgres_without_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = MaxMetric(
            name="max_metric_test",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_value()
        assert row_value["value"] == 501

    def test_should_return_max_column_value_postgres_with_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = MaxMetric(
            name="max_metric_test_1",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_value()
        assert row_value["value"] == 110

    def test_should_return_max_column_value_opensearch_without_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = MaxMetric(
            name="max_metric_test",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_value()
        assert row_value["value"] == 501

    def test_should_return_max_column_value_opensearch_with_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = MaxMetric(
            name="max_metric_test_1",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_value()
        assert row_value["value"] == 110

@pytest.mark.usefixtures("setup_data", "postgres_datasource", "opensearch_datasource")
class TestAvgColumnValueMetric:
    def test_should_return_avg_column_value_postgres_without_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = AvgMetric(
            name="avg_metric_test",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_value()
        assert float(row_value["value"]) == 141.40

    def test_should_return_avg_column_value_postgres_with_filter(
        self, postgres_datasource: PostgresSQLDatasource
    ):
        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=postgres_datasource,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_value()
        assert float(row_value["value"]) == 51.50

    def test_should_return_avg_column_value_opensearch_without_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = AvgMetric(
            name="avg_metric_test",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_value()
        assert float(row_value["value"]) == 141.40

    def test_should_return_avg_column_value_opensearch_with_filter(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=opensearch_datasource,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_value()
        assert float(row_value["value"]) == 51.50
