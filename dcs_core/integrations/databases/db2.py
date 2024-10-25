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

from dcs_core.integrations.utils.utils import ibm_db2_dll_files_loader

ibm_db2_dll_files_loader()
from typing import Any, Dict, List, Tuple, Union

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class DB2DataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the DB2 data source using SQLAlchemy
        """
        try:
            url = self._build_connection_url()
            engine = create_engine(url, echo=False)
            self.connection = engine.connect()
            return self.connection
        except SQLAlchemyError as e:
            raise DataChecksDataSourcesConnectionError(
                f"Failed to connect to DB2 data source: {str(e)}"
            )

    def _build_connection_url(self) -> str:
        """
        Build the SQLAlchemy connection URL for DB2
        """
        host = self.data_connection.get("host")
        port = self.data_connection.get("port")
        database = self.data_connection.get("database")
        username = self.data_connection.get("username")
        password = self.data_connection.get("password")

        url = f"db2+ibm_db://{username}:{password}@{host}:{port}/{database}"

        params = []
        if self.data_connection.get("security"):
            params.append(f"SECURITY={self.data_connection['security']}")
        if self.data_connection.get("protocol"):
            params.append(f"PROTOCOL={self.data_connection['protocol']}")
        if self.data_connection.get("schema"):
            params.append(f"CURRENTSCHEMA={self.data_connection.get('schema')}")
        if params:
            url += "?" + "&".join(params)

        return url

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

        if not regex_pattern and not predefined_regex_pattern:
            raise ValueError(
                "Either regex_pattern or predefined_regex_pattern should be provided"
            )

        if predefined_regex_pattern:
            regex = self.regex_patterns[predefined_regex_pattern]
        else:
            regex = regex_pattern

        regex_query = f"CASE WHEN REGEXP_LIKE({field}, '{regex}') THEN 1 ELSE 0 END"

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
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
        :param regex_pattern: regex pattern
        :param filters: filter condition
        :return: count of valid values and total count of rows.
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            validity_condition = (
                f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
            )
        else:
            validity_condition = (
                f"CASE WHEN REGEXP_LIKE({field}, '{regex_pattern}') THEN 1 ELSE 0 END"
            )

        query = f"""
            SELECT SUM({validity_condition}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters}
        """

        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_usa_state_code_validity(
        self,
        table: str,
        field: str,
        filters: str = None,
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

        regex_query = f"CASE WHEN REGEXP_LIKE{field}, '^[A-Z]{{2}}$' AND {field} IN ({valid_state_codes_str}) THEN 1 ELSE 0 END"

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
        operation: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param operation: Metric operation ("count" or "percent")
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Tuple containing valid count and total count (or percentage)
        """

        qualified_table_name = self.qualified_table_name(table)

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"REGEXP_LIKE({field}, '{timestamp_iso_regex}')"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
        WITH extracted_timestamps AS (
            SELECT
                {field},
                SUBSTR({field}, 1, 4) AS year,         -- Extract year
                SUBSTR({field}, 6, 2) AS month,        -- Extract month
                SUBSTR({field}, 9, 2) AS day,          -- Extract day
                SUBSTR({field}, 12, 2) AS hour,        -- Extract hour
                SUBSTR({field}, 15, 2) AS minute,      -- Extract minute
                SUBSTR({field}, 18, 2) AS second,      -- Extract second
                SUBSTR({field}, 20) AS timezone        -- Extract timezone
            FROM {qualified_table_name}
            {filters_clause}
        ),
        validated_timestamps AS (
            SELECT
                {field},
                CASE
                    WHEN
                        -- Validate each component with its specific rules
                        REGEXP_LIKE(year, '^\d{{4}}$') AND
                        REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                        REGEXP_LIKE(day, '^((0[1-9]|[12][0-9])|(30|31))$') AND
                        REGEXP_LIKE(hour, '^([01][0-9]|2[0-3])$') AND
                        REGEXP_LIKE(minute, '^[0-5][0-9]$') AND
                        REGEXP_LIKE(second, '^[0-5][0-9]$') AND
                        (timezone IS NULL OR REGEXP_LIKE(timezone, '^(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$')) AND
                        -- Additional check for days in months (e.g., February)
                        (
                            (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                            (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                            (month = '02' AND day BETWEEN '01' AND
                                CASE
                                    -- Handle leap years
                                    WHEN (CAST(year AS INT) % 400 = 0 OR (CAST(year AS INT) % 100 != 0 AND CAST(year AS INT) % 4 = 0)) THEN '29'
                                    ELSE '28'
                                END
                            )
                        )
                    THEN 1
                    ELSE 0
                END AS is_valid
            FROM extracted_timestamps
        )
        SELECT COUNT(*) AS valid_count
        FROM validated_timestamps
        WHERE is_valid = 1;
        """

        try:
            valid_count = self.fetchone(query)[0]
            total_count_query = (
                f"SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}"
            )
            total_count = self.fetchone(total_count_query)[0]

            if operation == "count":
                return valid_count, total_count
            elif operation == "percent":
                return (valid_count / total_count) * 100 if total_count > 0 else 0.0
            else:
                raise ValueError(f"Unknown operation: {operation}")

        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return 0, 0

    def query_timestamp_not_in_future_metric(
        self,
        table: str,
        field: str,
        operation: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param operation: Metric operation ("count" or "percent")
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Tuple containing count of valid timestamps not in the future and total count
        """

        qualified_table_name = self.qualified_table_name(table)

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"REGEXP_LIKE({field}, '{timestamp_iso_regex}')"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            WITH extracted_timestamps AS (
                SELECT
                    {field},
                    SUBSTRING({field}, 1, 4) AS year,        -- Extract year
                    SUBSTRING({field}, 6, 2) AS month,       -- Extract month
                    SUBSTRING({field}, 9, 2) AS day,         -- Extract day
                    SUBSTRING({field}, 12, 2) AS hour,       -- Extract hour
                    SUBSTRING({field}, 15, 2) AS minute,     -- Extract minute
                    SUBSTRING({field}, 18, 2) AS second,     -- Extract second
                    SUBSTRING({field}, 20) AS timezone       -- Extract timezone
                FROM {qualified_table_name}
                {filters_clause}
            ),
            validated_timestamps AS (
                SELECT
                    {field},
                    CASE
                        WHEN
                            REGEXP_LIKE(year, '^\d{{4}}$') AND
                            REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                            REGEXP_LIKE(day, '^((0[1-9]|[12][0-9])|(30|31))$') AND
                            REGEXP_LIKE(hour, '^([01][0-9]|2[0-3])$') AND
                            REGEXP_LIKE(minute, '^[0-5][0-9]$') AND
                            REGEXP_LIKE(second, '^[0-5][0-9]$') AND
                            (timezone IS NULL OR REGEXP_LIKE(timezone, '^(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$')) AND
                            (
                                (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                                (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                                (month = '02' AND day BETWEEN '01' AND
                                    CASE
                                        WHEN (CAST(year AS INTEGER) % 400 = 0 OR
                                            (CAST(year AS INTEGER) % 100 != 0 AND
                                            CAST(year AS INTEGER) % 4 = 0)) THEN '29'
                                        ELSE '28'
                                    END
                                )
                            )
                        THEN 1
                        ELSE 0
                    END AS is_valid
                FROM extracted_timestamps
            ),
            timestamps_not_in_future AS (
                SELECT *
                FROM validated_timestamps
                WHERE is_valid = 1 AND {regex_condition}
                AND TO_TIMESTAMP({field}, 'YYYY-MM-DD"T"HH24:MI:SS') <= CURRENT TIMESTAMP
            )
            SELECT COUNT(*) AS valid_count, (SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}) AS total_count
            FROM timestamps_not_in_future;
        """
        try:
            valid_count = self.fetchone(query)[0]
            total_count_query = (
                f"SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}"
            )
            total_count = self.fetchone(total_count_query)[0]

            if operation == "count":
                return valid_count, total_count
            elif operation == "percent":
                return (valid_count / total_count) * 100 if total_count > 0 else 0
            else:
                raise ValueError(f"Unknown operation: {operation}")

        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return 0, 0

    def query_timestamp_date_not_in_future_metric(
        self,
        table: str,
        field: str,
        operation: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param operation: Metric operation ("count" or "percent")
        :param predefined_regex: The regex pattern to use (e.g., "timestamp_iso")
        :param filters: Optional filter condition
        :return: Tuple containing count of valid dates not in the future and total count
        """

        qualified_table_name = self.qualified_table_name(table)

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"REGEXP_LIKE({field}, '{timestamp_iso_regex}')"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            WITH extracted_timestamps AS (
                SELECT
                    {field},
                    SUBSTRING({field}, 1, 4) AS year,        -- Extract year
                    SUBSTRING({field}, 6, 2) AS month,       -- Extract month
                    SUBSTRING({field}, 9, 2) AS day          -- Extract day
                FROM {qualified_table_name}
                {filters_clause}
            ),
            validated_dates AS (
                SELECT
                    {field},
                    CASE
                        WHEN
                            REGEXP_LIKE(year, '^\d{{4}}$') AND
                            REGEXP_LIKE(month, '^(0[1-9]|1[0-2])$') AND
                            REGEXP_LIKE(day, '^((0[1-9]|[12][0-9])|(30|31))$') AND
                            (
                                (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                                (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                                (month = '02' AND day BETWEEN '01' AND
                                    CASE
                                        WHEN (CAST(year AS INTEGER) % 400 = 0 OR
                                            (CAST(year AS INTEGER) % 100 != 0 AND
                                            CAST(year AS INTEGER) % 4 = 0)) THEN '29'
                                        ELSE '28'
                                    END
                                )
                            )
                        THEN 1
                        ELSE 0
                    END AS is_valid
                FROM extracted_timestamps
            ),
            dates_not_in_future AS (
                SELECT *
                FROM validated_dates
                WHERE is_valid = 1
                AND {regex_condition}
                AND DATE(TO_TIMESTAMP({field}, 'YYYY-MM-DD"T"HH24:MI:SS')) <= CURRENT DATE  -- Compare only the date part
            )
            SELECT COUNT(*) AS valid_count, (SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}) AS total_count
            FROM dates_not_in_future;
        """

        try:
            valid_count = self.fetchone(query)[0]
            total_count_query = (
                f"SELECT COUNT(*) FROM {qualified_table_name} {filters_clause}"
            )
            total_count = self.fetchone(total_count_query)[0]

            if operation == "count":
                return valid_count, total_count
            elif operation == "percent":
                return (valid_count / total_count) * 100 if total_count > 0 else 0
            else:
                raise ValueError(f"Unknown operation: {operation}")

        except Exception as e:
            logger.error(f"Failed to execute query: {str(e)}")
            return 0, 0
