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

from typing import Any, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.data_source_resource import RawColumnInfo
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class PostgresDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.DEFAULT_NUMERIC_PRECISION = 16383

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            url = URL.create(
                drivername="postgresql",
                username=self.data_connection.get("username"),
                password=self.data_connection.get("password"),
                host=self.data_connection.get("host"),
                port=self.data_connection.get("port"),
                database=self.data_connection.get("database"),
            )
            schema = self.data_connection.get("schema") or "public"
            engine = create_engine(
                url,
                connect_args={"options": f"-csearch_path={schema}"},
                isolation_level="AUTOCOMMIT",
            )
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to PostgresSQL data source: [{str(e)}]"
            )

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        if self.schema_name:
            return f'"{self.schema_name}"."{table_name}"'
        return f'"{table_name}"'

    def quote_column(self, column: str) -> str:
        """
        Quote the column name
        :param column: name of the column
        :return: quoted column name
        """
        return f'"{column}"'

    def query_get_table_names(
        self,
        schema: str | None = None,
        with_view: bool = False,
    ) -> List[str]:
        """
        Get the list of tables in the database.
        :param schema: optional schema name
        :return: list of table names
        """

        schema = schema or self.schema_name
        database = self.quote_database(self.database)

        if with_view:
            table_type_condition = "table_type IN ('BASE TABLE', 'VIEW')"
        else:
            table_type_condition = "table_type = 'BASE TABLE'"

        query = (
            f"SELECT table_name FROM {database}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND {table_type_condition}"
        )
        result = self.fetchall(query)
        return [row[0] for row in result]

    def query_get_table_columns(
        self,
        table: str,
        schema: str | None = None,
    ) -> RawColumnInfo:
        """
        Get the schema of a table.
        :param table: table name
        :return: RawColumnInfo object containing column information
        """
        schema = schema or self.schema_name
        info_schema_path = ["information_schema", "columns"]
        if self.database:
            database = self.quote_database(self.database)
            info_schema_path.insert(0, database)
        query = (
            f"SELECT column_name, data_type, datetime_precision, "
            f"CASE WHEN data_type = 'numeric' "
            f"THEN coalesce(numeric_precision, 131072 + {self.DEFAULT_NUMERIC_PRECISION}) "
            f"ELSE numeric_precision END AS numeric_precision, "
            f"CASE WHEN data_type = 'numeric' "
            f"THEN coalesce(numeric_scale, {self.DEFAULT_NUMERIC_PRECISION}) "
            f"ELSE numeric_scale END AS numeric_scale, "
            f"COALESCE(collation_name, NULL) AS collation_name, "
            f"CASE WHEN data_type = 'character varying' "
            f"THEN character_maximum_length END AS character_maximum_length "
            f"FROM {'.'.join(info_schema_path)} "
            f"WHERE table_name = '{table}' AND table_schema = '{schema}'"
        )
        rows = self.fetchall(query)
        if not rows:
            raise RuntimeError(
                f"{table}: Table, {schema}: Schema, does not exist, or has no columns"
            )

        column_info = {
            r[0]: RawColumnInfo(
                column_name=self.safe_get(r, 0),
                data_type=self.safe_get(r, 1),
                datetime_precision=self.safe_get(r, 2),
                numeric_precision=self.safe_get(r, 3),
                numeric_scale=self.safe_get(r, 4),
                collation_name=self.safe_get(r, 5),
                character_maximum_length=self.safe_get(r, 6),
            )
            for r in rows
        }
        return column_info
