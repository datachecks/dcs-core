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

from typing import Any, Dict, List, Tuple, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class MssqlDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            driver = (
                self.data_connection.get("driver") or "ODBC Driver 18 for SQL Server"
            )
            url = URL.create(
                drivername="mssql+pyodbc",
                username=self.data_connection.get("username"),
                password=self.data_connection.get("password"),
                host=self.data_connection.get("host"),
                port=self.data_connection.get("port", 1433),
                database=self.data_connection.get("database"),
                query={"driver": driver, "TrustServerCertificate": "YES"},
            )
            schema = self.data_connection.get("schema") or "dbo"
            # For osx have to install
            # brew install unixodbc
            # brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
            # brew update
            # brew install msodbcsql mssql-tools
            engine = create_engine(
                url,
                connect_args={"options": f"-csearch_path={schema}"},
                isolation_level="AUTOCOMMIT",
            )
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Mssql data source: [{str(e)}]"
            )

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
