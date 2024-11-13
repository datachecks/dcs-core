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

from typing import Any, Dict, Tuple, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.integrations.databases.db2 import DB2DataSource


class MysqlDataSource(DB2DataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.regex_patterns = {
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            "usa_phone": r"^\\+?1?[-.[:space:]]?\\(?[0-9]{3}\\)?[-.[:space:]]?[0-9]{3}[-.[:space:]]?[0-9]{4}$",
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
            "usa_zip_code": r"^[0-9B-DF-HJ-NP-TV-Z]{6}[0-9]$",
            "ssn": r"^(?!666|000|9\\d{2})\\d{3}-(?!00)\\d{2}-(?!0{4})\\d{4}$",
            "sedol": r"^[B-DF-HJ-NP-TV-XZ0-9]{6}[0-9]$",
            "lei": r"^[A-Z0-9]{18}[0-9]{2}$",
            "cusip": r"^[0-9A-Z]{9}$",
            "figi": r"^BBG[A-Z0-9]{9}$",
            "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
            "perm_id": r"^[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{4}[- ]?[0-9]{2,3}$",
        }

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            url = URL.create(
                drivername="mysql+pymysql",
                username=self.data_connection.get("username"),
                password=self.data_connection.get("password"),
                host=self.data_connection.get("host"),
                port=self.data_connection.get("port"),
                database=self.data_connection.get("database"),
            )
            engine = create_engine(
                url,
                isolation_level="AUTOCOMMIT",
            )
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Mysql data source: [{str(e)}]"
            )

    def query_get_distinct_count(
        self, table: str, field: str, filters: str = None
    ) -> int:
        """
        Get the distinct count value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT COUNT(DISTINCT {}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return self.fetchone(query)[0]

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
        rank = int(percentile * 100)

        query = f"""
            SELECT {field} FROM (
                SELECT {field}, NTILE(100) OVER (ORDER BY {field}) AS percentile_rank
                FROM {qualified_table_name}
                {f'WHERE {filters}' if filters else ''}
            ) AS ranked
            WHERE percentile_rank = {rank}
            ORDER BY {field}
            LIMIT 1
        """

        result = self.fetchone(query)
        return round(result[0], 2) if result and result[0] is not None else None

    def query_negative_metric(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        qualified_table_name = self.qualified_table_name(table)

        negative_query = (
            f"SELECT COUNT(*) FROM {qualified_table_name} WHERE {field} < 0"
        )

        if filters:
            negative_query += f" AND {filters}"

        total_count_query = f"SELECT COUNT(*) FROM {qualified_table_name}"

        if filters:
            total_count_query += f" WHERE {filters}"

        if operation == "percent":
            query = f"SELECT (CAST(({negative_query}) AS float) / CAST(({total_count_query}) AS float)) * 100"
        else:
            query = negative_query

        result = self.fetchone(query)[0]
        return round(result, 2) if operation == "percent" else result

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
            sql_function = "MAX(LENGTH"
        elif metric.lower() == "min":
            sql_function = "MIN(LENGTH"
        elif metric.lower() == "avg":
            sql_function = "AVG(LENGTH"
        else:
            raise ValueError(
                f"Invalid metric '{metric}'. Choose from 'max', 'min', or 'avg'."
            )

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

        regex_query = f"CASE WHEN {field} REGEXP '{regex}' THEN 1 ELSE 0 END"
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

        regex_query = f"""
            CASE WHEN REGEXP_LIKE({field}, '^[A-Z]{{2}}$') AND UPPER({field}) IN ({valid_state_codes_str}) THEN 1 ELSE 0 END
        """

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_timestamp_metric(self):
        raise NotImplementedError("Method not implemented for MySQLDataSource")

    def query_timestamp_not_in_future_metric(self):
        raise NotImplementedError("Method not implemented for MySQLDataSource")

    def query_timestamp_date_not_in_future_metric(self):
        raise NotImplementedError("Method not implemented for MySQLDataSource")
