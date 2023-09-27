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
from _decimal import Decimal
from loguru import logger
from sqlalchemy import text

from datachecks.core.common.models.configuration import (
    DataSourceConnectionConfiguration,
)
from datachecks.integrations.databases.postgres import PostgresDataSource
from tests.utils import create_postgres_connection

OPEN_SEARCH_DATA_SOURCE_NAME = "test_open_search_data_source"
POSTGRES_DATA_SOURCE_NAME = "test_postgres_data_source"


@pytest.mark.usefixtures("postgres_datasource", "pgsql_connection_configuration")
class TestSQLDataSourceTableColumnMetadata:
    TABEL_NAME = "numeric_metric_test"

    @pytest.fixture(scope="class", autouse=True)
    def setup_tables(
        self, pgsql_connection_configuration: DataSourceConnectionConfiguration
    ):
        postgresql_connection = create_postgres_connection(
            pgsql_connection_configuration
        )
        try:
            postgresql_connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.TABEL_NAME} (
                        name VARCHAR(50),
                        age INT,
                        income FLOAT,
                        is_active BOOLEAN,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        location TEXT,
                        extra JSONB,
                        number_of_children SERIAL,
                        short_name CHARACTER(5),
                        house_locations VARCHAR(50)[]
                    )
                """
                )
            )
            postgresql_connection.execute(
                text(
                    f"""
                CREATE TABLE IF NOT EXISTS {self.TABEL_NAME}_2 ( name VARCHAR(50), age INT )
            """
                )
            )
            postgresql_connection.commit()
            yield True
        except Exception as e:
            print(e)
        finally:
            postgresql_connection.execute(
                text(f"DROP TABLE IF EXISTS {self.TABEL_NAME}")
            )
            postgresql_connection.commit()

            postgresql_connection.close()

    def test_table_column_metadata(self, postgres_datasource: PostgresDataSource):
        table_column_metadata = postgres_datasource.query_get_column_metadata(
            table_name=self.TABEL_NAME
        )
        logger.info(table_column_metadata)

        assert table_column_metadata["name"] == "str"
        assert table_column_metadata["age"] == "int"
        assert table_column_metadata["income"] == "float"
        assert table_column_metadata["is_active"] == "bool"
        assert table_column_metadata["created_at"] == "datetime"
        assert table_column_metadata["updated_at"] == "datetime"
        assert table_column_metadata["location"] == "str"
        assert table_column_metadata["extra"] == "dict"
        assert table_column_metadata["number_of_children"] == "int"
        assert table_column_metadata["short_name"] == "str"
        assert table_column_metadata["house_locations"] == "list"

    def test_table_metadata(self, postgres_datasource: PostgresDataSource):
        tables = postgres_datasource.query_get_table_metadata()

        assert self.TABEL_NAME in tables
        assert f"{self.TABEL_NAME}_2" in tables


@pytest.mark.usefixtures("postgres_datasource", "pgsql_connection_configuration")
class TestSQLDatasourceQueries:
    TABLE_NAME = "numeric_metric_test_one"

    @pytest.fixture(scope="class", autouse=True)
    def setup_tables(
        self, pgsql_connection_configuration: DataSourceConnectionConfiguration
    ):
        postgresql_connection = create_postgres_connection(
            pgsql_connection_configuration
        )
        try:
            postgresql_connection.execute(
                text(
                    f"""
                        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                            name VARCHAR(50), last_fight timestamp, age INTEGER, weight FLOAT, description VARCHAR(100)
                        )
                    """
                )
            )

            utc_now = datetime.datetime.utcnow()
            insert_query = f"""
                INSERT INTO {self.TABLE_NAME} VALUES
                ('thor', '{(utc_now - datetime.timedelta(days=10)).strftime("%Y-%m-%d")}', 1500, NULL, 'thor hammer'),
                ('captain america', '{(utc_now - datetime.timedelta(days=3)).strftime("%Y-%m-%d")}', 90, 80, 'shield'),
                ('iron man', '{(utc_now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")}', 50, 70, 'suit'),
                ('hawk eye', '{(utc_now - datetime.timedelta(days=5)).strftime("%Y-%m-%d")}', 40, 60, 'bow'),
                ('clark kent', '{(utc_now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")}', 35, 50, ''),
                ('black widow', '{(utc_now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")}', 35, 50, '')
            """

            postgresql_connection.execute(text(insert_query))

            postgresql_connection.commit()
            yield True
        except Exception as e:
            print(e)
        finally:
            postgresql_connection.execute(
                text(f"DROP TABLE IF EXISTS {self.TABLE_NAME}")
            )
            postgresql_connection.commit()

            postgresql_connection.close()

    def test_should_return_numeric_profile(
        self, postgres_datasource: PostgresDataSource
    ):
        profile = postgres_datasource.profiling_sql_aggregates_numeric(
            self.TABLE_NAME, "age"
        )
        assert profile["min"] == 35
        assert profile["max"] == 1500
        assert profile["sum"] == 1750
        assert round(profile["avg"], 2) == Decimal("291.67")

    def test_should_return_text_profile(self, postgres_datasource: PostgresDataSource):
        profile = postgres_datasource.profiling_sql_aggregates_string(
            self.TABLE_NAME, "name"
        )
        assert profile["distinct_count"] == 6
        assert profile["missing_count"] == 0
        assert profile["max_length"] == 15

    def test_should_return_avg_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_avg(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 767.5

    def test_should_return_min_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_min(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 35

    def test_should_return_max_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_max(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1500

    def test_should_return_sum_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_sum(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1535

    def test_should_return_variance_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_variance(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1073112.5

    def test_should_return_stddev_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_stddev(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == round(Decimal(1035.91), 2)

    def test_should_return_duplicate_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_duplicate_count(
            table=self.TABLE_NAME,
            field="age",
        )
        assert result == 1

    def test_should_return_null_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_null_count(
            table=self.TABLE_NAME,
            field="weight",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1

    def test_should_return_empty_string_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_empty_string_count(
            table=self.TABLE_NAME,
            field="description",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1

    def test_should_return_empty_string_percentage_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_empty_string_percentage(
            table=self.TABLE_NAME,
            field="description",
            filters="name in ('thor', 'black widow')",
        )
        assert round(result, 2) == 50.0

    def test_should_return_distinct_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_distinct_count(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 2

    def test_should_return_null_percentage_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_null_percentage(
            table=self.TABLE_NAME,
            field="weight",
            filters="name in ('thor', 'black widow')",
        )
        assert round(result, 2) == 50.0

    def test_should_return_time_diff_in_second(
        self, postgres_datasource: PostgresDataSource
    ):
        time_diff = postgres_datasource.query_get_time_diff(
            self.TABLE_NAME, field="last_fight"
        )
        assert time_diff >= 3 * 24 * 3600

    def test_should_return_row_count(self, postgres_datasource: PostgresDataSource):
        row_count = postgres_datasource.query_get_row_count(self.TABLE_NAME)
        assert row_count == 6
