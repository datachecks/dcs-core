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

import random
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

from loguru import logger
from sqlalchemy import create_engine

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class SybaseDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.regex_patterns = {
            "uuid": r"%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%",
            "usa_phone": r"%[0-9][0-9][0-9] [0-9][0-9][0-9] [0-9][0-9][0-9][0-9]%",
            "email": r"%[a-zA-Z0-9._%+-]@[a-zA-Z0-9.-]%.[a-zA-Z]%",
            "usa_zip_code": r"[0-9][0-9][0-9][0-9][0-9]%",
            "ssn": r"%[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]%",
            "sedol": r"[B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][0-9]",
            "lei": r"[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9][0-9]",
            "cusip": r"[0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z]",
            "figi": r"BBG[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]",
            "isin": r"[A-Z][A-Z][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9]",
            "perm_id": r"%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9]%",
        }

    def connect(self) -> Any:
        try:
            driver = self.data_connection.get("driver") or "FreeTDS"
            username = self.data_connection.get("username")
            password = self.data_connection.get("password")
            host = self.data_connection.get("host")
            port = self.data_connection.get("port", 5000)
            database = self.data_connection.get("database")
            schema = self.data_connection.get("schema", "dbo") or "dbo"
            cp_driver = (
                driver.replace("{", "")
                .replace("}", "")
                .replace(" ", "")
                .strip()
                .lower()
            )
            if cp_driver.startswith("adaptive"):
                engine = create_engine(
                    f"sybase+pyodbc://:@",
                    connect_args={
                        "DRIVER": driver,
                        "UID": username,
                        "PWD": password,
                        "SERVER": host,
                        "PORT": port,
                        "DATABASE": database,
                        "options": f"-csearch_path={schema}",
                    },
                    isolation_level="AUTOCOMMIT",
                )
            else:
                connection_string = (
                    f"sybase+pyodbc://{username}:{password}@{host}:{port}/{database}"
                    f"?driver={driver}"
                )
                connection_string_with_schema = (
                    f"{connection_string}&options=-csearch_path={schema}"
                )
                engine = create_engine(
                    connection_string_with_schema, isolation_level="AUTOCOMMIT"
                )

            self.connection = engine.connect()
            return self.connection

        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Sybase data source: [{str(e)}]"
            )

    def convert_regex_to_sybase_pattern(self, regex_pattern: str) -> str:
        """
        Convert a regex pattern into a Sybase-compatible LIKE pattern.
        """
        sybase_pattern = re.sub(r"([%_])", r"[\1]", regex_pattern)

        sybase_pattern = sybase_pattern.replace(".*", "%")
        sybase_pattern = sybase_pattern.replace(".", "_")
        sybase_pattern = sybase_pattern.replace(".+", "_%")

        sybase_pattern = sybase_pattern.replace("?", "_")

        sybase_pattern = re.sub(
            r"\[([^\]]+)\]", lambda m: f"%[{m.group(1)}]%", sybase_pattern
        )

        sybase_pattern = sybase_pattern.lstrip("^").rstrip("$")

        return sybase_pattern

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
            validation_query = f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
        else:
            sybase_pattern = self.convert_regex_to_sybase_pattern(regex_pattern)
            validation_query = (
                f"CASE WHEN {field} LIKE '{sybase_pattern}' THEN 1 ELSE 0 END"
            )

        query = f"""
            SELECT SUM({validation_query}) AS valid_count, COUNT(*) as total_count
            FROM {qualified_table_name}
            {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_percentile(
        self, table: str, field: str, percentile: float, filters: str = None
    ) -> float:
        raise NotImplementedError("Method not implemented for Sybase data source")

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

        query = f"""
            SELECT COUNT(*) AS space_count
            FROM {qualified_table_name}
            WHERE {field} LIKE '% %' OR {field} LIKE '%' + CHAR(160) + '%'
        """

        if filters:
            query += f" AND {filters}"

        total_query = f"SELECT COUNT(*) AS total_count FROM {qualified_table_name}"
        if filters:
            total_query += f" WHERE {filters}"

        space_count = self.fetchone(query)[0]
        total_count = self.fetchone(total_query)[0]

        if operation == "percent":
            return round((space_count / total_count) * 100, 2) if total_count > 0 else 0

        return space_count if space_count is not None else 0

    def query_get_null_keyword_count(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the count of NULL-like values (specific keywords) in the specified column.
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of NULL-like keyword values
        """
        qualified_table_name = self.qualified_table_name(table)

        # Query that checks for both NULL and specific NULL-like values
        query = f"""
                SELECT SUM(CASE
                            WHEN {field} IS NULL OR LOWER({field}) IN ('nothing', 'nil', 'null', 'none', 'n/a')
                            THEN 1
                            ELSE 0
                        END) AS null_count, COUNT(*) AS total_count
                FROM {qualified_table_name}
            """
        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)

        if result:
            if operation == "percent":
                return round((result[0] / result[1]) * 100, 2) if result[1] > 0 else 0
            return result[0]

        return 0

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

        if metric.lower() == "max":
            sql_function = "MAX(LEN"
        elif metric.lower() == "min":
            sql_function = "MIN(LEN"
        elif metric.lower() == "avg":
            sql_function = "AVG(CAST(LEN(" + field + ") AS FLOAT))"
        else:
            raise ValueError(
                f"Invalid metric '{metric}'. Choose from 'max', 'min', or 'avg'."
            )
        if metric.lower() == "avg":
            query = f"SELECT {sql_function} FROM {qualified_table_name}"
        else:
            query = f"SELECT {sql_function}({field})) FROM {qualified_table_name}"
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

        if not regex_pattern and not predefined_regex_pattern:
            raise ValueError(
                "Either regex_pattern or predefined_regex_pattern should be provided"
            )

        if predefined_regex_pattern:
            length_query = None
            pt = self.regex_patterns[predefined_regex_pattern]
            if predefined_regex_pattern == "uuid":
                length_query = f"LEN({field}) = 36"
            elif predefined_regex_pattern == "perm_id":
                length_query = f"LEN({field}) BETWEEN 19 AND 23 "
            elif predefined_regex_pattern == "lei":
                length_query = f"LEN({field}) = 20"
            elif predefined_regex_pattern == "cusip":
                length_query = f"LEN({field}) = 9"
            elif predefined_regex_pattern == "figi":
                length_query = f"LEN({field}) = 12"
            elif predefined_regex_pattern == "isin":
                length_query = f"LEN({field}) = 12"
            elif predefined_regex_pattern == "sedol":
                length_query = f"LEN({field}) = 7"
            elif predefined_regex_pattern == "ssn":
                length_query = f"LEN({field}) = 11"
            elif predefined_regex_pattern == "usa_zip_code":
                query = f"""
                    SELECT
                        SUM(CASE
                            WHEN PATINDEX('%[0-9][0-9][0-9][0-9][0-9]%', CAST({field} AS VARCHAR)) > 0
                            AND (LEN(CAST({field} AS VARCHAR)) = 5 OR LEN(CAST({field} AS VARCHAR)) = 9)
                            THEN 1
                            ELSE 0
                        END) AS valid_count,
                        COUNT(*) AS total_count
                    FROM {qualified_table_name} {filters};
                    """
                result = self.fetchone(query)
                return result[0], result[1]
            if not length_query:
                regex_query = f"PATINDEX('{pt}', {field}) > 0"
            else:
                regex_query = f"PATINDEX('{pt}', {field}) > 0 AND {length_query}"
        else:
            regex_query = self.convert_regex_to_sybase_pattern(regex_pattern)
        query = f"""
            SELECT
                SUM(CASE
                        WHEN {regex_query}
                        THEN 1
                        ELSE 0
                    END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_time_diff(self, table: str, field: str) -> int:
        """
        Get the time difference
        :param table: name of the index
        :param field: field name of updated time column
        :return: time difference in seconds
        """
        qualified_table_name = self.qualified_table_name(table)
        query = f"""
            SELECT TOP 1 {field}
            FROM {qualified_table_name}
            ORDER BY {field} DESC;
        """
        result = self.fetchone(query)
        if result:
            updated_time = result[0]
            if isinstance(updated_time, str):
                updated_time = datetime.strptime(updated_time, "%Y-%m-%d %H:%M:%S.%f")
            return int((datetime.utcnow() - updated_time).total_seconds())
        return 0

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

        temp_table_suffix = f"{int(time.time())}_{random.randint(1000, 9999)}"
        extracted_table = f"#extracted_timestamps_{temp_table_suffix}"
        validated_table = f"#validated_timestamps_{temp_table_suffix}"

        if predefined_regex == "timestamp_iso":
            filters_clause = f"WHERE {filters}" if filters else ""

            query = f"""
                -- Extract timestamp components
                SELECT
                    {field},
                    LEFT(CONVERT(VARCHAR, {field}, 120), 4) AS year,       -- Extract year
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 6, 2) AS month,  -- Extract month
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 9, 2) AS day,    -- Extract day
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 12, 2) AS hour,  -- Extract hour
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 15, 2) AS minute, -- Extract minute
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 18, 2) AS second  -- Extract second
                INTO {extracted_table}
                FROM {qualified_table_name}
                {filters_clause};

                -- Validate timestamps and calculate the is_valid flag
                SELECT
                    {field},
                    CASE
                        WHEN
                            -- Validate year, month, and day formats
                            year LIKE '[0-9][0-9][0-9][0-9]' AND
                            month LIKE '[0-1][0-9]' AND month BETWEEN '01' AND '12' AND
                            day LIKE '[0-3][0-9]' AND day BETWEEN '01' AND
                                CASE
                                    -- Check for days in each month
                                    WHEN month IN ('01', '03', '05', '07', '08', '10', '12') THEN '31'
                                    WHEN month IN ('04', '06', '09', '11') THEN '30'
                                    WHEN month = '02' THEN
                                        CASE
                                            -- Check for leap years
                                            WHEN (CAST(year AS INT) % 400 = 0 OR (CAST(year AS INT) % 100 != 0 AND CAST(year AS INT) % 4 = 0)) THEN '29'
                                            ELSE '28'
                                        END
                                    ELSE '00' -- Invalid month
                                END AND
                            -- Validate time components
                            hour LIKE '[0-2][0-9]' AND hour BETWEEN '00' AND '23' AND
                            minute LIKE '[0-5][0-9]' AND
                            second LIKE '[0-5][0-9]'
                        THEN 1
                        ELSE 0
                    END AS is_valid
                INTO {validated_table}
                FROM {extracted_table};

                -- Get the counts
                SELECT
                    SUM(is_valid) AS valid_count,
                    COUNT(*) AS total_count
                FROM {validated_table};
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

        if predefined_regex != "timestamp_iso":
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            SELECT
                SUM(CASE
                    WHEN
                        -- Validate year, month, day
                        DATEPART(yy, {field}) BETWEEN 1 AND 9999 AND
                        DATEPART(mm, {field}) BETWEEN 1 AND 12 AND
                        DATEPART(dd, {field}) BETWEEN 1 AND
                            CASE
                                WHEN DATEPART(mm, {field}) IN (1, 3, 5, 7, 8, 10, 12) THEN 31
                                WHEN DATEPART(mm, {field}) IN (4, 6, 9, 11) THEN 30
                                WHEN DATEPART(mm, {field}) = 2 THEN
                                    CASE
                                        WHEN DATEPART(yy, {field}) % 400 = 0 OR
                                            (DATEPART(yy, {field}) % 4 = 0 AND DATEPART(yy, {field}) % 100 != 0) THEN 29
                                        ELSE 28
                                    END
                                ELSE 0
                            END AND
                        -- Validate hour, minute, second
                        DATEPART(hh, {field}) BETWEEN 0 AND 23 AND
                        DATEPART(mi, {field}) BETWEEN 0 AND 59 AND
                        DATEPART(ss, {field}) BETWEEN 0 AND 59 AND
                        -- Ensure timestamp is not in the future
                        {field} <= GETDATE()
                    THEN 1
                    ELSE 0
                END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters_clause}
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
        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            SELECT
                SUM(CASE
                    WHEN
                        -- Validate year, month, and day
                        DATEPART(yy, {field}) BETWEEN 1 AND 9999 AND
                        DATEPART(mm, {field}) BETWEEN 1 AND 12 AND
                        DATEPART(dd, {field}) BETWEEN 1 AND
                            CASE
                                WHEN DATEPART(mm, {field}) IN (1, 3, 5, 7, 8, 10, 12) THEN 31
                                WHEN DATEPART(mm, {field}) IN (4, 6, 9, 11) THEN 30
                                WHEN DATEPART(mm, {field}) = 2 THEN
                                    CASE
                                        WHEN DATEPART(yy, {field}) % 400 = 0 OR
                                            (DATEPART(yy, {field}) % 4 = 0 AND DATEPART(yy, {field}) % 100 != 0) THEN 29
                                        ELSE 28
                                    END
                                ELSE 0
                            END AND
                        -- Validate hour, minute, and second
                        DATEPART(hh, {field}) BETWEEN 0 AND 23 AND
                        DATEPART(mi, {field}) BETWEEN 0 AND 59 AND
                        DATEPART(ss, {field}) BETWEEN 0 AND 59 AND
                        -- Ensure the timestamp is not in the future
                        {field} <= GETDATE()
                    THEN 1
                    ELSE 0
                END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters_clause}
        """

        try:
            result = self.fetchone(query)
            valid_count = result[0]
            total_count = result[1]

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0
