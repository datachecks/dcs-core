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

from loguru import logger
from sqlalchemy import create_engine, text

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.data_source_resource import RawColumnInfo
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class OracleDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

        self.regex_patterns = {
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            "usa_phone": r"^\(\d{3}\) \d{3}-\d{4}$",
            "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            "usa_zip_code": r"^[0-9]{5}(?:-[0-9]{4})?$",
            "ssn": r"^[0-6]\d{2}-(0[1-9]|[1-9]\d)-([1-9]\d{3}|\d{4})$",
            "sedol": r"^[A-Z0-9]{6}\d$",
            "lei": r"^[A-Z0-9]{18}[0-9]{2}$",
            "cusip": r"^[0-9A-Z]{8}[0-9]$",
            "figi": r"^BBG[A-Z0-9]{9}$",
            "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
            "perm_id": r"^\d{4}([- ]?)\d{4}\1\d{4}\1\d{4}([- ]?)\d{3}$",
        }

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            engine = create_engine(
                f"oracle+oracledb://:@",
                thick_mode=False,
                connect_args={
                    "user": self.data_connection.get("username"),
                    "password": self.data_connection.get("password"),
                    "host": self.data_connection.get("host"),
                    "port": self.data_connection.get("port"),
                    "service_name": self.data_connection.get("service_name"),
                },
            )
            self.schema_name = self.data_connection.get(
                "schema"
            ) or self.data_connection.get("username")
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Oracle data source: [{str(e)}]"
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
        query = database_version_query or "SELECT BANNER FROM v$version"
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

        if with_view:
            query = (
                f"SELECT TABLE_NAME, 'TABLE' AS OBJECT_TYPE FROM ALL_ALL_TABLES WHERE OWNER = '{schema}' "
                f"UNION "
                f"SELECT VIEW_NAME AS TABLE_NAME, 'VIEW' AS OBJECT_TYPE FROM ALL_VIEWS WHERE OWNER = '{schema}'"
            )
        else:
            query = f"SELECT TABLE_NAME, 'TABLE' AS OBJECT_TYPE FROM ALL_ALL_TABLES WHERE OWNER = '{schema}'"

        rows = self.fetchall(query)

        if with_view:
            result = {"table": [], "view": []}
            if rows:
                for row in rows:
                    object_name = row[0]
                    object_type = row[1].strip() if row[1] else row[1]

                    if object_type == "TABLE":
                        result["table"].append(object_name)
                    elif object_type == "VIEW":
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
        Get index information for a table in Oracle DB.
        :param table: Table name
        :param schema: Optional schema name
        :return: Dictionary with index details
        """
        schema = schema or self.schema_name
        table = table.upper()
        schema = schema.upper()

        query = f"""
            SELECT
                ind.index_name,
                ind.index_type,
                col.column_name,
                col.column_position AS column_order
            FROM
                ALL_INDEXES ind
            JOIN
                ALL_IND_COLUMNS col ON ind.index_name = col.index_name AND ind.table_name = col.table_name AND ind.owner = col.index_owner
            WHERE
                ind.table_name = '{table}'
                AND ind.owner = '{schema}'
            ORDER BY
                ind.index_name, col.column_position
        """
        rows = self.fetchall(query)

        if not rows:
            raise RuntimeError(
                f"No index information found for table '{table}' in schema '{schema}'."
            )

        pk_query = f"""
            SELECT acc.column_name
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc ON ac.constraint_name = acc.constraint_name AND ac.owner = acc.owner
            WHERE ac.constraint_type = 'P'
            AND ac.table_name = '{table}'
            AND ac.owner = '{schema}'
            ORDER BY acc.position
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
        query = (
            f"SELECT column_name, data_type, 6 as datetime_precision, data_precision as numeric_precision, "
            f"data_scale as numeric_scale, NULL as collation_name, char_length as character_maximum_length "
            f"FROM ALL_TAB_COLUMNS WHERE table_name = '{table}' AND owner = '{schema}'"
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
        query = (
            complete_query
            or f"SELECT * FROM ({query}) subquery ORDER BY 1 FETCH NEXT {limit} ROWS ONLY"
        )

        result = self.connection.execute(text(query))
        rows = result.fetchmany(limit)

        if with_column_names:
            column_names = result.keys()
            return rows, list(column_names)
        else:
            return rows, None

    def query_valid_invalid_values_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        filters: str = None,
        values: List[str] = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid and invalid values
        :param table: table name
        :param field: column name
        :param values: list of valid values
        :param regex_pattern: regex pattern
        :param filters: filter condition
        :return: count of valid/invalid values and total count of valid/invalid values
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            regex_query = f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
        else:
            regex_query = (
                f"CASE WHEN REGEXP_LIKE({field}, '{regex_pattern}') THEN 1 ELSE 0 END"
            )

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters}
        """

        result = self.fetchone(query)
        return result[0], result[1]

    def query_string_pattern_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        predefined_regex_pattern: str = None,
        filters: str = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid values based on the regex pattern
        :param table: table name
        :param field: column name
        :param regex_pattern: regex pattern
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

        if predefined_regex_pattern:
            regex_condition = f"REGEXP_LIKE({field}, '{self.regex_patterns[predefined_regex_pattern]}')"
        else:
            regex_condition = f"REGEXP_LIKE({field}, '{regex_pattern}')"

        regex_query = f"CASE WHEN {regex_condition} THEN 1 ELSE 0 END"

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
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

        regex_query = (
            f"CASE WHEN REGEXP_LIKE({field}, '^[A-Z]{{2}}$') "
            f"AND {field} IN ({valid_state_codes_str}) THEN 1 ELSE 0 END"
        )

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """

        result = self.fetchone(query)
        return result[0], result[1]

    def query_timestamp_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Tuple containing valid count and total count (or percentage)
        """

        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        if predefined_regex == "timestamp_iso":
            filters_clause = f"WHERE {filters}" if filters else ""

            query = f"""
                WITH extracted_timestamps AS (
                    SELECT
                        {field},
                        TO_CHAR({field}, 'YYYY') AS year,
                        TO_CHAR({field}, 'MM') AS month,
                        TO_CHAR({field}, 'DD') AS day,
                        TO_CHAR({field}, 'HH24') AS hour,
                        TO_CHAR({field}, 'MI') AS minute,
                        TO_CHAR({field}, 'SS') AS second
                    FROM {qualified_table_name}
                    {filters_clause}
                ),
                validated_timestamps AS (
                    SELECT
                        {field},
                        CASE
                            WHEN
                                REGEXP_LIKE(year, '^\\d{{4}}$') AND
                                REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                                REGEXP_LIKE(day, '^([0-2][0-9]|3[01])$') AND
                                (
                                    (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                                    (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                                    (month = '02' AND day BETWEEN '01' AND
                                        CASE
                                            WHEN MOD(TO_NUMBER(year), 400) = 0 OR
                                                (MOD(TO_NUMBER(year), 4) = 0 AND MOD(TO_NUMBER(year), 100) != 0) THEN '29'
                                            ELSE '28'
                                        END
                                    )
                                ) AND
                                REGEXP_LIKE(hour, '^(0[0-9]|1[0-9]|2[0-3])$') AND
                                REGEXP_LIKE(minute, '^[0-5][0-9]$') AND
                                REGEXP_LIKE(second, '^[0-5][0-9]$')
                            THEN 1
                            ELSE 0
                        END AS is_valid
                    FROM extracted_timestamps
                )
                SELECT SUM(is_valid) AS valid_count, COUNT(*) AS total_count
                FROM validated_timestamps
            """
            try:
                result = self.fetchone(query)
                valid_count = result[0]
                total_count = result[1]

                return valid_count, total_count
            except Exception as e:
                logger.error(f"Error occurred: {e}")
                return 0, 0
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

    def query_timestamp_not_in_future_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Count of valid timestamps not in the future and total count or percentage
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"REGEXP_LIKE({field}, '{timestamp_iso_regex}')"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            WITH extracted_timestamps AS (
                SELECT
                    TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS') AS formatted_{field},
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}', 1, 1) AS year,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}-(\d{{2}})', 1, 1, NULL, 1) AS month,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}-\d{{2}}-(\d{{2}})', 1, 1, NULL, 1) AS day,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ' (\d{{2}})', 1, 1, NULL, 1) AS hour,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ':\d{{2}}:(\d{{2}})', 1, 1, NULL, 1) AS minute,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ':(\d{{2}})$', 1, 1, NULL, 1) AS second
                FROM {qualified_table_name}
                {filters_clause}
            ),
            validated_timestamps AS (
                SELECT
                    formatted_{field},
                    CASE
                        WHEN
                            REGEXP_LIKE(year, '^\d{{4}}$') AND
                            REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                            REGEXP_LIKE(day, '^([0-2][0-9]|3[01])$') AND
                            (
                                (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                                (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                                (month = '02' AND day BETWEEN '01' AND
                                    CASE
                                        WHEN MOD(TO_NUMBER(year), 400) = 0 OR
                                            (MOD(TO_NUMBER(year), 4) = 0 AND MOD(TO_NUMBER(year), 100) != 0) THEN '29'
                                        ELSE '28'
                                    END
                                )
                            ) AND
                            REGEXP_LIKE(hour, '^(0[0-9]|1[0-9]|2[0-3])$') AND
                            REGEXP_LIKE(minute, '^[0-5][0-9]$') AND
                            REGEXP_LIKE(second, '^[0-5][0-9]$')
                        THEN 1
                        ELSE 0
                    END AS is_valid
                FROM extracted_timestamps
            ),
            timestamps_not_in_future AS (
                SELECT *
                FROM validated_timestamps
                WHERE is_valid = 1 AND TO_TIMESTAMP(formatted_{field}, 'YYYY-MM-DD HH24:MI:SS') <= CURRENT_TIMESTAMP
            )
            SELECT
                (SELECT COUNT(*) FROM timestamps_not_in_future) AS valid_count,
                (SELECT COUNT(*) FROM {qualified_table_name}) AS total_count
            FROM dual
        """
        try:
            result = self.fetchone(query)
            valid_count = result[0]
            total_count = result[1]

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0

    def query_timestamp_date_not_in_future_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: The regex pattern to use (e.g., "timestamp_iso")
        :param filters: Optional filter condition
        :return: Tuple containing count of valid dates not in the future and total count
        """

        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            WITH extracted_timestamps AS (
                SELECT
                    TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS') AS formatted_{field},
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}', 1, 1) AS year,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}-(\d{{2}})', 1, 1, NULL, 1) AS month,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), '^\d{{4}}-\d{{2}}-(\d{{2}})', 1, 1, NULL, 1) AS day,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ' (\d{{2}})', 1, 1, NULL, 1) AS hour,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ':\d{{2}}:(\d{{2}})', 1, 1, NULL, 1) AS minute,
                    REGEXP_SUBSTR(TO_CHAR({field}, 'YYYY-MM-DD HH24:MI:SS'), ':(\d{{2}})$', 1, 1, NULL, 1) AS second
                FROM {qualified_table_name}
                {filters_clause}
            ),
            validated_timestamps AS (
                SELECT
                    formatted_{field},
                    CASE
                        WHEN
                            REGEXP_LIKE(year, '^\d{{4}}$') AND
                            REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                            REGEXP_LIKE(day, '^([0-2][0-9]|3[01])$') AND
                            (
                                (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                                (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                                (month = '02' AND day BETWEEN '01' AND
                                    CASE
                                        WHEN MOD(TO_NUMBER(year), 400) = 0 OR
                                            (MOD(TO_NUMBER(year), 4) = 0 AND MOD(TO_NUMBER(year), 100) != 0) THEN '29'
                                        ELSE '28'
                                    END
                                )
                            ) AND
                            REGEXP_LIKE(hour, '^(0[0-9]|1[0-9]|2[0-3])$') AND
                            REGEXP_LIKE(minute, '^[0-5][0-9]$') AND
                            REGEXP_LIKE(second, '^[0-5][0-9]$')
                        THEN 1
                        ELSE 0
                    END AS is_valid
                FROM extracted_timestamps
            ),
            validated_dates AS (
                SELECT
                    formatted_{field},
                    is_valid
                FROM validated_timestamps
                WHERE is_valid = 1
            ),
            dates_not_in_future AS (
                SELECT *
                FROM validated_dates
                WHERE is_valid = 1
                AND REGEXP_LIKE(formatted_{field}, '^\d{{4}}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d{{2}}):([0-5][0-9]):([0-5][0-9])(\.\d{{1,3}})?$')
                AND TO_TIMESTAMP(formatted_{field}, 'YYYY-MM-DD HH24:MI:SS') <= CURRENT_TIMESTAMP
            )
            SELECT
                (SELECT COUNT(*) FROM dates_not_in_future) AS valid_count,
                (SELECT COUNT(*) FROM {qualified_table_name}) AS total_count
            FROM dual
        """

        try:
            valid_count = self.fetchone(query)[0]
            total_count_query = (
                f"SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}"
            )
            total_count = self.fetchone(total_count_query)[0]

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0

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
            SELECT {field} from {qualified_table_name} ORDER BY {field} DESC LIMIT 1;
        """
        query = f"""
            SELECT {field}
            FROM (
                SELECT {field}
                FROM {qualified_table_name}
                ORDER BY {field} DESC
            )
            WHERE ROWNUM = 1
        """
        result = self.fetchone(query)
        if result:
            return int(abs(datetime.utcnow() - result[0]).total_seconds())
        return 0

    def query_get_all_space_count(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the count of rows where the specified column contains only spaces.
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of rows with only spaces
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        query = f"""
            SELECT
                COUNT(CASE WHEN TRIM({field}) IS NULL OR TRIM({field}) = '' THEN 1 END) AS space_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
        """

        if filters:
            query += f"WHERE {filters}"

        result = self.fetchone(query)

        if operation == "percent":
            return round((result[0] / result[1]) * 100) if result[1] > 0 else 0

        return result[0] if result else 0
