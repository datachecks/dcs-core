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

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
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

    def query_get_database_version(
        self, database_version_query: Optional[str] = None
    ) -> str:
        """
        Get the database version
        :return: version string
        """
        query = database_version_query or "SELECT version()"
        result = self.fetchone(query)[0]
        return result if result else None

    def query_get_table_names(
        self,
        schema: str | None = None,
        with_view: bool = False,
    ) -> dict:
        """
        Get the list of tables in the database.
        :param schema: optional schema name
        :param with_view: whether to include views
        :return: dictionary with table names and optionally view names
        """

        schema = schema or self.schema_name
        database = self.quote_database(self.database)

        if with_view:
            table_type_condition = "table_type IN ('BASE TABLE', 'VIEW')"
        else:
            table_type_condition = "table_type = 'BASE TABLE'"

        query = (
            f"SELECT table_name, table_type FROM {database}.information_schema.tables "
            f"WHERE table_schema = '{schema}' AND {table_type_condition}"
        )
        rows = self.fetchall(query)

        if with_view:
            result = {"table": [], "view": []}
            if rows:
                for row in rows:
                    table_name = row[0]
                    table_type = row[1].strip() if row[1] else row[1]

                    if table_type == "BASE TABLE":
                        result["table"].append(table_name)
                    elif table_type == "VIEW":
                        result["view"].append(table_name)
        else:
            result = {"table": []}
            if rows:
                result["table"] = [row[0] for row in rows]

        return result

    def query_get_table_indexes(
        self, table: str, schema: str | None = None
    ) -> dict[str, dict]:
        """
        Get index information for a table in PostgreSQL DB.
        :param table: Table name
        :param schema: Optional schema name
        :return: Dictionary with index details
        """
        schema = schema or self.schema_name
        table = table.lower()
        schema = schema.lower()

        query = f"""
            SELECT
                i.relname AS index_name,
                am.amname AS index_type,
                a.attname AS column_name,
                x.n AS column_order
            FROM
                pg_class t
            JOIN
                pg_namespace ns ON ns.oid = t.relnamespace
            JOIN
                pg_index ix ON t.oid = ix.indrelid
            JOIN
                pg_class i ON i.oid = ix.indexrelid
            JOIN
                pg_am am ON i.relam = am.oid
            JOIN
                LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n)
                    ON TRUE
            JOIN
                pg_attribute a ON a.attnum = x.attnum AND a.attrelid = t.oid
            WHERE
                t.relkind = 'r'
                AND t.relname = '{table}'
                AND ns.nspname = '{schema}'
            ORDER BY
                i.relname, x.n
        """
        rows = self.fetchall(query)

        if not rows:
            raise RuntimeError(
                f"No index information found for table '{table}' in schema '{schema}'."
            )

        pk_query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.constraint_schema = kcu.constraint_schema
            AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = '{table}'
            AND tc.table_schema = '{schema}'
            ORDER BY kcu.ordinal_position
        """
        pk_rows = self.fetchall(pk_query)
        pk_columns = [row[0].strip() for row in pk_rows] if pk_rows else []
        pk_columns_set = set(pk_columns)

        indexes = {}
        for row in rows:
            index_name = row[0]
            index_type = row[1]
            column_info = {
                "column_name": self.safe_get(row, 2),
                "column_order": self.safe_get(row, 3),
            }
            if index_name not in indexes:
                indexes[index_name] = {"columns": [], "index_type": index_type}
            indexes[index_name]["columns"].append(column_info)

        for index_name, idx in indexes.items():
            index_columns = [col["column_name"].strip() for col in idx["columns"]]
            index_columns_set = set(index_columns)
            idx["is_primary_key"] = pk_columns_set == index_columns_set and len(
                index_columns
            ) == len(pk_columns)
        return indexes

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

    def fetch_rows(
        self,
        query: str,
        limit: int = 1,
        with_column_names: bool = False,
        complete_query: Optional[str] = None,
    ) -> Tuple[List, Optional[List[str]]]:
        """
        Fetch rows from the database.

        :param query: SQL query to execute.
        :param limit: Number of rows to fetch.
        :param with_column_names: Whether to include column names in the result.
        :return: Tuple of (rows, column_names or None)
        """
        query = complete_query or f"SELECT * FROM ({query}) AS subquery LIMIT {limit}"

        result = self.connection.execute(text(query))
        rows = result.fetchmany(limit)

        if with_column_names:
            column_names = result.keys()
            return rows, list(column_names)
        else:
            return rows, None
