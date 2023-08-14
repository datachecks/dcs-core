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
from loguru import logger
from sqlalchemy import text

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration
from datachecks.core.datasource.postgres import PostgresSQLDatasource
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

    def test_table_column_metadata(self, postgres_datasource: PostgresSQLDatasource):
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

    def test_table_metadata(self, postgres_datasource: PostgresSQLDatasource):
        tables = postgres_datasource.query_get_table_metadata()

        assert self.TABEL_NAME in tables
        assert f"{self.TABEL_NAME}_2" in tables
