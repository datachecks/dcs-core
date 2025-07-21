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

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pyodbc
from loguru import logger

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.data_source_resource import RawColumnInfo
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class MssqlDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.regex_patterns = {
            "uuid": r"[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%",
            "usa_phone": r"^(\+1[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$",
            "email": r"%[a-zA-Z0-9._%+-]@[a-zA-Z0-9.-]%.[a-zA-Z]%",
            "usa_zip_code": r"^[0-9]{5}(?:-[0-9]{4})?$",
            "ssn": r"^(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}$",
            "sedol": r"[B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][0-9]",
            "lei": r"[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9][0-9]",
            "cusip": r"[0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z]",
            "figi": r"BBG[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]",
            "isin": r"[A-Z][A-Z][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9]",
            "perm_id": r"^\d{4}([- ]?)\d{4}\1\d{4}\1\d{4}([- ]?)\d{3}$",
        }

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        driver = self.data_connection.get("driver") or "ODBC Driver 18 for SQL Server"
        host = self.data_connection.get("host")
        port = self.data_connection.get("port")
        database = self.data_connection.get("database")
        username = self.data_connection.get("username")
        password = self.data_connection.get("password")
        server = self.data_connection.get("server")

        connection_params = self._build_connection_params(
            driver=driver, database=database, username=username, password=password
        )

        return self._establish_connection(connection_params, host, server, port)

    def _prepare_driver_string(self, driver: str) -> str:
        """Ensure driver string is properly formatted with braces."""
        return f"{{{driver}}}" if not driver.startswith("{") else driver

    def _build_connection_params(
        self, driver: str, database: str, username: str, password: str
    ) -> dict:
        return {
            "DRIVER": self._prepare_driver_string(driver),
            "DATABASE": database,
            "UID": username,
            "PWD": password,
            "TrustServerCertificate": "yes",
        }

    def _establish_connection(
        self, conn_dict: dict, host: str, server: str, port: str
    ) -> Any:
        connection_attempts = [
            (host, True),  # host with port
            (host, False),  # host without port
            (server, True),  # server with port
            (server, False),  # server without port
        ]

        for _, (server_value, use_port) in enumerate(connection_attempts, 1):
            if not server_value:
                continue

            try:
                conn_dict["SERVER"] = (
                    f"{server_value},{port}" if use_port and port else server_value
                )
                self.connection = pyodbc.connect(**conn_dict)
                logger.info(f"Connected to MSSQL database using {conn_dict['SERVER']}")
                return self.connection
            except Exception:
                continue

        raise DataChecksDataSourcesConnectionError(
            message="Failed to connect to Mssql data source: [All connection attempts failed]"
        )

    def fetchall(self, query):
        return self.connection.cursor().execute(query).fetchall()

    def fetchone(self, query):
        return self.connection.cursor().execute(query).fetchone()

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        if self.schema_name:
            return f"[{self.schema_name}].[{table_name}]"
        return f"[{table_name}]"

    def quote_column(self, column: str) -> str:
        """
        Quote the column name
        :param column: name of the column
        :return: quoted column name
        """
        return f"[{column}]"

    def query_get_table_names(
        self, schema: str | None = None, with_view: bool = False
    ) -> dict:
        """
        Get the list of tables in the database.
        :param schema: optional schema name
        :param with_view: whether to include views
        :return: dictionary with table names and optionally view names
        """
        schema = schema or self.schema_name

        if with_view:
            object_types = "IN ('U', 'V')"
        else:
            object_types = "= 'U'"

        query = f"SELECT o.name AS table_name, o.type FROM sys.objects o JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE o.type {object_types} AND s.name = '{schema}' ORDER BY o.name"

        rows = self.fetchall(query)

        if with_view:
            result = {"table": [], "view": []}
            if rows:
                for row in rows:
                    object_name = row[0]
                    object_type = row[1].strip() if row[1] else row[1]

                    if object_type == "U":
                        result["table"].append(object_name)
                    elif object_type == "V":
                        result["view"].append(object_name)
        else:
            result = {"table": []}
            if rows:
                result["table"] = [row[0] for row in rows]

        return result

    def query_get_table_indexes(
        self, table: str, schema: str | None = None
    ) -> dict[str, dict]:
        """
        Get index information for a table in MSSQL DB.
        :param table: Table name
        :param schema: Optional schema name
        :return: Dictionary with index details
        """
        schema = schema or self.schema_name
        table = table.upper()
        schema = schema.upper()

        query = f"""
            SELECT
                i.name AS index_name,
                i.type_desc AS index_type,
                c.name AS column_name,
                ic.key_ordinal AS column_order
            FROM
                sys.indexes i
            JOIN
                sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN
                sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN
                sys.tables t ON t.object_id = i.object_id
            JOIN
                sys.schemas s ON t.schema_id = s.schema_id
            WHERE
                t.name = '{table}'
                AND s.name = '{schema}'
                AND i.is_hypothetical = 0
            ORDER BY
                i.name, ic.key_ordinal
        """

        rows = self.fetchall(query)

        if not rows:
            raise RuntimeError(
                f"No index information found for table '{table}' in schema '{schema}'."
            )

        pk_query = f"""
            SELECT c.name AS column_name
            FROM
                sys.key_constraints kc
            JOIN
                sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            JOIN
                sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN
                sys.tables t ON t.object_id = kc.parent_object_id
            JOIN
                sys.schemas s ON t.schema_id = s.schema_id
            WHERE
                kc.type = 'PK'
                AND t.name = '{table}'
                AND s.name = '{schema}'
            ORDER BY ic.key_ordinal
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
        self, table: str, schema: str | None = None
    ) -> RawColumnInfo:
        """
        Get the schema of a table.
        :param table: table name
        :return: RawColumnInfo object containing column information
        """
        schema = schema or self.schema_name
        database = self.quote_database(self.database)
        query = (
            "SELECT column_name, data_type, ISNULL(datetime_precision, 0) AS datetime_precision, ISNULL(numeric_precision, 0) AS numeric_precision, ISNULL(numeric_scale, 0) AS numeric_scale, collation_name, ISNULL(character_maximum_length, 0) AS character_maximum_length "
            f"FROM {database}.information_schema.columns "
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
        Fetch rows from the database using pyodbc.

        :param query: SQL query to execute.
        :param limit: Number of rows to fetch.
        :param with_column_names: Whether to include column names in the result.
        :return: Tuple of (rows, column_names or None)
        """
        query = (
            complete_query
            or f"SELECT * FROM ({query}) AS subquery ORDER BY 1 OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
        )
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchmany(limit)

        if with_column_names:
            column_names = [column[0] for column in cursor.description]
            return rows, column_names
        else:
            return rows, None

    def regex_to_sql_condition(self, regex_pattern: str, field: str) -> str:
        """
        Convert regex patterns to SQL Server conditions
        """
        if (
            regex_pattern.startswith("^") and regex_pattern.endswith("$")
        ) or "|" in regex_pattern:
            pattern = regex_pattern.strip("^$")
            if pattern.startswith("(") and pattern.endswith(")"):
                pattern = pattern[1:-1]

            if "|" in pattern:
                values = [f"'{val.strip()}'" for val in pattern.split("|")]
                return f"IIF({field} IN ({', '.join(values)}), 1, 0)"

        pattern = regex_pattern
        if pattern.startswith("^"):
            pattern = pattern[1:]
        if pattern.endswith("$"):
            pattern = pattern[:-1]

        pattern = pattern.replace(".*", "%").replace(".+", "%").replace(".", "_")

        return f"IIF({field} LIKE '{pattern}', 1, 0)"

    def query_get_variance(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the variance value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT VAR({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.fetchone(query)[0], 2)

    def query_get_stddev(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the standard deviation value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT STDEV({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.fetchone(query)[0], 2)

    def query_get_percentile(
        self, table: str, field: str, percentile: float, filters: str = None
    ) -> float:
        """
        Get the specified percentile value of a numeric column in a table.
        :param table: table name
        :param field: column name
        :param percentile: percentile to calculate (e.g., 0.2 for 20th percentile)
        :param filters: filter condition
        :return: the value at the specified percentile
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = f"""
            SELECT PERCENTILE_CONT({percentile}) WITHIN GROUP (ORDER BY {field})
            OVER () AS percentile_value
            FROM {qualified_table_name}
        """
        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)
        return round(result[0], 2) if result and result[0] is not None else None

    def query_get_null_keyword_count(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the count of NULL-like values (specific keywords) in the specified column for MSSQL.
        :param table: table name
        :param field: column name
        :param operation: type of operation ('count' or 'percent')
        :param filters: filter condition
        :return: count (int) or percentage (float) of NULL-like keyword values
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        query = f"""
            SELECT
                SUM(CASE
                    WHEN {field} IS NULL
                    OR LTRIM(RTRIM(LOWER(ISNULL({field}, '')))) IN ('nothing', 'nil', 'null', 'none', 'n/a', '')
                    THEN 1
                    ELSE 0
                END) AS null_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
        """

        if filters:
            query += f" AND {filters}"

        result = self.fetchone(query)

        if not result or not result[1]:
            return 0

        null_count = int(result[0] if result[0] is not None else 0)
        total_count = int(result[1])

        if operation == "percent":
            return (
                round((null_count / total_count) * 100, 2) if total_count > 0 else 0.0
            )

        return null_count

    def query_get_string_length_metric(
        self, table: str, field: str, metric: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the string length metric (max, min, avg) in a column of a table.

        :param table: table name
        :param field: column name
        :param metric: the metric to calculate ('max', 'min', 'avg')
        :param filters: filter condition
        :return: the calculated metric as int for 'max' and 'min', float for 'avg'
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        if metric.lower() == "max":
            sql_function = "MAX(LEN"
        elif metric.lower() == "min":
            sql_function = "MIN(LEN"
        elif metric.lower() == "avg":
            sql_function = "AVG(LEN"
        else:
            raise ValueError(
                f"Invalid metric '{metric}'. Choose from 'max', 'min', or 'avg'."
            )

        if metric.lower() == "avg":
            query = (
                f'SELECT AVG(CAST(LEN("{field}") AS FLOAT)) FROM {qualified_table_name}'
            )
        else:
            query = f'SELECT {sql_function}("{field}")) FROM {qualified_table_name}'

        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)[0]
        return round(result, 2) if metric.lower() == "avg" else result

    def query_string_pattern_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        predefined_regex_pattern: str = None,
        filters: str = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid values based on the regex pattern.
        :param table: table name
        :param field: column name
        :param regex_pattern: custom regex pattern
        :param predefined_regex_pattern: predefined regex pattern
        :param filters: filter condition
        :return: count of valid values, count of total row count
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        if not regex_pattern and not predefined_regex_pattern:
            raise ValueError(
                "Either regex_pattern or predefined_regex_pattern should be provided"
            )
        if regex_pattern:
            regex = regex_pattern
        else:
            regex = self.regex_patterns[predefined_regex_pattern]

        regex = self.regex_to_sql_condition(regex, field)

        query = f"""
            SELECT SUM(CAST({regex} AS BIGINT)) AS valid_count,
                   COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters}
        """
        if predefined_regex_pattern == "perm_id":
            query = f"""
                SELECT
                SUM(CASE
                    WHEN {field} LIKE '[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9]'
                    OR {field} LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                    THEN 1
                    ELSE 0
                END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name};
            """
        elif predefined_regex_pattern == "ssn":
            query = f"""
            SELECT
                SUM(CASE
                        WHEN {field} LIKE '[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]'
                            AND LEFT({field}, 3) NOT IN ('000', '666')
                            AND LEFT({field}, 1) != '9'
                            AND SUBSTRING({field}, 5, 2) != '00'
                            AND RIGHT({field}, 4) != '0000'
                        THEN 1
                        ELSE 0
                    END) AS valid_count,
            COUNT(*) AS total_count
            FROM {qualified_table_name}
            """
        elif predefined_regex_pattern == "usa_phone":
            query = f"""
            SELECT
                SUM(CASE
                        WHEN ({field} LIKE '+1 [0-9][0-9][0-9] [0-9][0-9][0-9] [0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1-[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1.[0-9][0-9][0-9].[0-9][0-9][0-9].[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '([0-9][0-9][0-9]) [0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9] [0-9][0-9][0-9] [0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9].[0-9][0-9][0-9].[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9]-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1 ([0-9][0-9][0-9]) [0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '([0-9][0-9][0-9])[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1 ([0-9][0-9][0-9])[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '+1 ([0-9][0-9][0-9]).[0-9][0-9][0-9].[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '([0-9][0-9][0-9]).[0-9][0-9][0-9].[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '([0-9][0-9][0-9])-[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9] [0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]'
                        OR {field} LIKE '[0-9][0-9][0-9].[0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]')
                        THEN 1
                        ELSE 0
                    END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name};

        """
        elif predefined_regex_pattern == "usa_zip_code":
            query = f"""
            SELECT
                SUM(CASE
                    WHEN PATINDEX('%[0-9][0-9][0-9][0-9][0-9]%[-][0-9][0-9][0-9][0-9]%', CAST({field} AS VARCHAR)) > 0
                    OR PATINDEX('%[0-9][0-9][0-9][0-9][0-9]%', CAST({field} AS VARCHAR)) > 0
                    THEN 1 ELSE 0 END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name};
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_valid_invalid_values_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        filters: str = None,
        values: List[str] = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid and invalid values for a specified column.
        :param table: table name
        :param field: column name
        :param values: list of valid values
        :param regex_pattern: regex pattern (will be converted to SQL Server pattern)
        :param filters: filter condition
        :return: count of valid values and total count of rows.
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            validity_condition = f"IIF({field} IN ({values_str}), 1, 0)"
        elif regex_pattern:
            validity_condition = self.regex_to_sql_condition(regex_pattern, field)
        else:
            raise ValueError("Either 'values' or 'regex_pattern' must be provided.")

        query = f"""
            SELECT SUM(CAST({validity_condition} AS BIGINT)) AS valid_count,
                   COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters}
        """

        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_usa_state_code_validity(
        self, table: str, field: str, filters: str = None
    ) -> Tuple[int, int]:
        """
        Get the count of valid USA state codes
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of valid state codes, count of total row count
        """
        valid_state_codes_str = ", ".join(
            f"'{code}'" for code in self.valid_state_codes
        )

        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        regex_query = f"""
                CASE
                    WHEN {field} IS NULL THEN 0
                    WHEN {field} IN ({valid_state_codes_str})
                    THEN 1
                    ELSE 0
                END"""

        query = f"""
                SELECT
                    SUM(CAST({regex_query} AS BIGINT)) AS valid_count,
                    COUNT(*) AS total_count
                FROM {qualified_table_name}
                {filters}
            """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_timestamp_metric(self):
        raise NotImplementedError("Method not implemented for MssqlDataSource")

    def query_timestamp_not_in_future_metric(self):
        raise NotImplementedError("Method not implemented for MssqlDataSource")

    def query_timestamp_date_not_in_future_metric(self):
        raise NotImplementedError("Method not implemented for MssqlDataSource")

    def query_get_time_diff(self, table: str, field: str) -> int:
        """
        Get the time difference
        :param table: name of the index
        :param field: field name of updated time column
        :return: time difference in seconds
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = f"""
            SELECT TOP 1 {field} FROM {qualified_table_name} ORDER BY {field} DESC;
        """
        result = self.fetchone(query)
        if result:
            updated_time = result[0]
            if isinstance(updated_time, str):
                updated_time = datetime.strptime(updated_time, "%Y-%m-%d %H:%M:%S.%f")
            return int((datetime.utcnow() - updated_time).total_seconds())
        return 0
