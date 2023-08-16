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
from unittest.mock import Mock

import pytest
from opensearchpy import OpenSearch
from sqlalchemy import Connection, text

from datachecks.core.common.models.metric import MetricsType
from datachecks.core.configuration.configuration import (
    DataSourceConnectionConfiguration,
)
from datachecks.core.datasource.opensearch import OpenSearchSearchIndexDataSource
from datachecks.core.datasource.postgres import PostgresSQLDatasource
from datachecks.core.datasource.search_datasource import SearchIndexDataSource
from datachecks.core.datasource.sql_datasource import SQLDatasource
from datachecks.core.metric.numeric_metric import AvgMetric, MaxMetric, MinMetric
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
        opensearch_client.indices.delete(index="numeric_metric_test", ignore=[400, 404])
        postgresql_connection.execute(text("DROP TABLE IF EXISTS numeric_metric_test"))
        postgresql_connection.commit()

        opensearch_client.close()
        postgresql_connection.close()


class TestMinColumnValueMetric:
    def test_should_return_min_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"where_clause": "age >= 100 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 100, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13
        assert row_value.field_name == "age"


class TestMaxColumnValueMetric:
    def test_should_return_max_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51


class TestAvgColumnValueMetric:
    def test_should_return_avg_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDatasource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3
