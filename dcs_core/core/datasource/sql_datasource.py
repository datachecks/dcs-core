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
from typing import Dict, List, Optional, Tuple, Union

from loguru import logger
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from dcs_core.core.datasource.base import DataSource


class SQLDataSource(DataSource):
    """
    Abstract class for SQL data sources
    """

    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

        self.connection: Union[Connection, None] = None
        self.database: str = data_connection.get("database")
        self.use_sa_text_query = True
        self.schema_name = data_connection.get("schema", None)
        self.regex_patterns = {
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            "usa_phone": r"^(\+1[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$",
            "email": r"^(?!.*\.\.)(?!.*@.*@)[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "usa_zip_code": r"^[0-9]{5}(?:-[0-9]{4})?$",
            "ssn": r"^(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}$",
            "sedol": r"^[B-DF-HJ-NP-TV-XZ0-9]{6}[0-9]$",
            "lei": r"^[A-Z0-9]{18}[0-9]{2}$",
            "cusip": r"^[0-9A-Z]{9}$",
            "figi": r"^BBG[A-Z0-9]{9}$",
            "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
            "perm_id": r"^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{3}$",
        }

        self.valid_state_codes = [
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
        ]

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.connection is not None

    def close(self):
        self.connection.close()
        try:
            self.connection.engine.dispose()
        except Exception as e:
            logger.error(f"Failed to close the connection: {str(e)}")

    def fetchall(self, query):
        if self.use_sa_text_query:
            return self.connection.execute(text(query)).fetchall()
        return self.connection.execute(query).fetchall()

    def fetchone(self, query):
        if self.use_sa_text_query:
            return self.connection.execute(text(query)).fetchone()
        return self.connection.execute(query).fetchone()

    def safe_get(self, lst, idx, default=None):
        return lst[idx] if 0 <= idx < len(lst) else default

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        if self.schema_name:
            return f"[{self.schema_name}].[{table_name}]"
        return f"[{table_name}]"

    def quote_database(self, database: str) -> str:
        """
        Quote the database name
        :param database: name of the database
        :return: quoted database name
        """
        return f'"{database}"'

    def quote_column(self, column: str) -> str:
        """
        Quote the column name
        :param column: name of the column
        :return: quoted column name
        """
        return f"[{column}]"

    def query_get_database_version(
        self, database_version_query: Optional[str] = None
    ) -> str:
        """
        Get the database version
        :return: version string
        """
        query = database_version_query or "SELECT @@version"
        result = self.fetchone(query)[0]
        return result if result else None

    def query_get_column_metadata(self, table_name: str) -> Dict[str, str]:
        """
        Get the column metadata
        :param table_name: name of the table
        :return: query for column metadata
        """
        results_: Dict[str, str] = {}

        columns = inspect(self.connection.engine).get_columns(table_name)
        for column in columns:
            results_[column["name"]] = column["type"].python_type.__name__

        return results_

    def query_get_table_metadata(self) -> List[str]:
        """
        Get the table metadata
        :return: query for table metadata
        """
        return inspect(self.connection.engine).get_table_names()

    def query_get_row_count(self, table: str, filters: str = None) -> int:
        """
        Get the row count
        :param table: name of the table
        :param filters: optional filter
        """
        qualified_table_name = self.qualified_table_name(table)
        query = f"SELECT COUNT(*) FROM {qualified_table_name}"
        if filters:
            query += f" WHERE {filters}"
        return self.fetchone(query)[0]

    def query_get_custom_sql(self, query: str) -> Union[int, float, None]:
        """
        Get the first row of the custom sql query
        :param query: custom sql query
        """
        row = self.fetchone(query)
        if row is not None:
            return row[0]
        else:
            return None

    def query_get_max(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the max value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        query = "SELECT MAX({}) FROM {}".format(field, qualified_table_name)

        if filters:
            query += " WHERE {}".format(filters)
        var = self.fetchone(query)[0]
        return var

    def query_get_min(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the min value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT MIN({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return self.fetchone(query)[0]

    def query_get_avg(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the average value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT AVG({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.fetchone(query)[0], 2)

    def query_get_sum(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the sum value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT SUM({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.fetchone(query)[0], 2)

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
        query = "SELECT VAR_SAMP({}) FROM {}".format(field, qualified_table_name)
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
        query = "SELECT STDDEV_SAMP({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.fetchone(query)[0], 2)

    def query_get_null_count(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the null count
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(
            qualified_table_name, field
        )
        if filters:
            query += " AND {}".format(filters)
        return self.fetchone(query)[0]

    def query_get_empty_string_count(
        self, table: str, field: str, filters: str = None
    ) -> int:
        """
        Get the count of empty strings in a column of a table
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of empty strings
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT COUNT(*) FROM {} WHERE {} = ''".format(
            qualified_table_name, field
        )
        if filters:
            query += " AND {}".format(filters)
        result = self.fetchone(query)
        return result[0] if result else 0

    def query_get_empty_string_percentage(
        self, table: str, field: str, filters: str = None
    ) -> float:
        """
        Get the empty string percentage in a column of a table
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: empty string percentage
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT SUM(CASE WHEN {} = '' THEN 1 ELSE 0 END) AS empty_string_count, COUNT(*) AS total_count FROM {}".format(
            field, qualified_table_name
        )

        if filters:
            query += " WHERE {}".format(filters)

        result = self.fetchone(query)
        if result and result[1] > 0:
            return round((result[0] / result[1]) * 100, 2)
        return 0.0

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
        field = self.quote_column(field)
        query = "SELECT COUNT(DISTINCT {}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return self.fetchone(query)[0]

    def query_get_null_percentage(
        self, table: str, field: str, filters: str = None
    ) -> int:
        """
        Get the null percentage
         :param table: table name
         :param field: column name
         :param filters: filter condition
         :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = "SELECT SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS null_count, COUNT(*) AS total_count FROM {}".format(
            field, qualified_table_name
        )

        if filters:
            query += " WHERE {}".format(filters)

        result = self.fetchone(query)
        if result:
            return round((result[0] / result[1]) * 100, 2)
        return 0

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
        result = self.fetchone(query)
        if result:
            return int((datetime.utcnow() - result[0]).total_seconds())
        return 0

    def profiling_sql_aggregates_numeric(
        self, table_name: str, column_name: str
    ) -> Dict:
        column_name = f'"{column_name}"'
        qualified_table_name = self.qualified_table_name(table_name)
        query = f"""
            SELECT
                avg({column_name}) as avg,
                min({column_name}) as min,
                max({column_name}) as max,
                sum({column_name}) as sum,
                stddev_samp({column_name}) as stddev,
                var_samp({column_name}) as variance,
                count(distinct({column_name})) as distinct_count,
                sum(case when {column_name} is null then 1 else 0 end) as missing_count
            FROM {qualified_table_name}
            """

        result = self.fetchone(query)
        return {
            "avg": result[0],
            "min": result[1],
            "max": result[2],
            "sum": result[3],
            "stddev": result[4],
            "variance": result[5],
            "distinct_count": result[6],
            "missing_count": result[7],
        }

    def profiling_sql_aggregates_string(
        self, table_name: str, column_name: str
    ) -> Dict:
        column_name = f'"{column_name}"'
        qualified_table_name = self.qualified_table_name(table_name)
        query = f"""
            SELECT
                count(distinct({column_name})) as distinct_count,
                sum(case when {column_name} is null then 1 else 0 end) as missing_count,
                max(length({column_name})) as max_length,
                min(length({column_name})) as min_length,
                avg(length({column_name})) as avg_length
            FROM {qualified_table_name}
            """

        result = self.fetchone(query)
        return {
            "distinct_count": result[0],
            "missing_count": result[1],
            "max_length": result[2],
            "min_length": result[3],
            "avg_length": result[4],
        }

    def query_get_duplicate_count(
        self, table: str, field: str, filters: str = None
    ) -> int:
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = f"""
            SELECT
            count(*) as duplicate_count
            FROM {qualified_table_name}
            {filters}
            GROUP BY {field}
            HAVING COUNT(*) > 1
            """

        result = self.fetchall(query)
        return len(result) if result else 0

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
            regex_query = f"case when {field} ~ '{self.regex_patterns[predefined_regex_pattern]}' then 1 else 0 end"
        else:
            regex_query = f"case when {field} ~ '{regex_pattern}' then 1 else 0 end"

        query = f"""
            select sum({regex_query}) as valid_count, count(*) as total_count
            from {qualified_table_name} {filters}
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
        field = self.quote_column(field)
        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            regex_query = f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
        else:
            regex_query = f"CASE WHEN {field} ~ '{regex_pattern}' THEN 1 ELSE 0 END"
        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) as total_count
            FROM {qualified_table_name}
            {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

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

        regex_query = f"CASE WHEN {field} ~ '^[A-Z]{{2}}$' AND {field} IN ({valid_state_codes_str}) THEN 1 ELSE 0 END"

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """

        result = self.fetchone(query)
        return result[0], result[1]

    def query_geolocation_metric(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        valid_query = f"SELECT COUNT({field}) FROM {qualified_table_name} WHERE {field} IS NOT NULL AND {field} "

        if field.lower().startswith("lat"):
            valid_query += "BETWEEN -90 AND 90"
        elif field.lower().startswith("lon"):
            valid_query += "BETWEEN -180 AND 180"

        if filters:
            valid_query += f" AND {filters}"

        valid_count = self.fetchone(valid_query)[0]

        if operation == "percent":
            total_query = f"SELECT COUNT(*) FROM {qualified_table_name}"
            if filters:
                total_query += f" WHERE {filters}"

            total_count = self.fetchone(total_query)[0]

            result = (valid_count / total_count) * 100 if total_count > 0 else 0
            return round(result, 2)

        return valid_count

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
        query = f"SELECT PERCENTILE_DISC({percentile}) WITHIN GROUP (ORDER BY {field}) FROM {qualified_table_name}"
        if filters:
            query += f" WHERE {filters}"
        return round(self.fetchone(query)[0], 2)

    def query_zero_metric(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        zero_query = f"SELECT COUNT(*) FROM {qualified_table_name} WHERE {field} = 0"

        if filters:
            zero_query += f" AND {filters}"

        if operation == "percent":
            total_count_query = f"SELECT COUNT(*) FROM {qualified_table_name}"
            if filters:
                total_count_query += f" WHERE {filters}"

            zero_count = self.fetchone(zero_query)[0]
            total_count = self.fetchone(total_count_query)[0]

            if total_count == 0:
                return 0.0

            result = (zero_count / total_count) * 100
            return round(result, 2)
        else:
            result = self.fetchone(zero_query)[0]
            return result

    def query_negative_metric(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        negative_query = (
            f"SELECT COUNT(*) FROM {qualified_table_name} WHERE {field} < 0"
        )

        if filters:
            negative_query += f" AND {filters}"

        total_count_query = f"SELECT COUNT(*) FROM {qualified_table_name}"

        if filters:
            total_count_query += f" WHERE {filters}"

        if operation == "percent":
            query = f"SELECT (CAST(({negative_query}) AS float) / CAST(({total_count_query}) AS float)) * 100 FROM {qualified_table_name}"
        else:
            query = negative_query

        result = self.fetchone(query)[0]
        return round(result, 2) if operation == "percent" else result

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

        query = f"""SELECT COUNT(CASE WHEN TRIM({field}) = '' THEN 1 END) AS space_count,COUNT(*) AS total_count FROM {qualified_table_name}
        """

        if filters:
            query += f" AND {filters}"

        result = self.fetchone(query)

        if operation == "percent":
            return round((result[0] / result[1]) * 100) if result[1] > 0 else 0

        return result[0] if result else 0

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
        field = self.quote_column(field)

        query = f""" SELECT SUM(CASE WHEN LOWER({field}) IN ('nothing', 'nil', 'null', 'none', 'n/a', null) THEN 1 ELSE 0 END) AS null_count,COUNT(*) AS total_count
                   FROM {qualified_table_name}"""

        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)

        if operation == "percent":
            return round((result[0] / result[1]) * 100, 2) if result[1] > 0 else 0

        return result[0] if result else 0

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

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"{field} ~ '{timestamp_iso_regex}'"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
         WITH extracted_timestamps AS (
         SELECT
            {field},
            SUBSTRING({field} FROM '^(\d{{4}})') AS year,        -- Extract year
            SUBSTRING({field} FROM '^\d{{4}}-(\d{{2}})') AS month, -- Extract month
            SUBSTRING({field} FROM '^\d{{4}}-\d{{2}}-(\d{{2}})') AS day, -- Extract day
            SUBSTRING({field} FROM 'T(\d{{2}})') AS hour,       -- Extract hour
            SUBSTRING({field} FROM 'T\d{{2}}:(\d{{2}})') AS minute, -- Extract minute
            SUBSTRING({field} FROM 'T\d{{2}}:\d{{2}}:(\d{{2}})') AS second, -- Extract second
            SUBSTRING({field} FROM '([+-]\d{{2}}:\d{{2}}|Z)$') AS timezone -- Extract timezone
         FROM {qualified_table_name}
         {filters_clause}
        ),
         validated_timestamps AS (
         SELECT
            {field},
            CASE
                WHEN
                    -- Validate each component with its specific rules
                    year ~ '^\d{{4}}$' AND
                    month ~ '^(0[1-9]|1[0-2])$' AND
                    day ~ '^((0[1-9]|[12][0-9])|(30|31))$' AND
                    hour ~ '^([01][0-9]|2[0-3])$' AND
                    minute ~ '^[0-5][0-9]$' AND
                    second ~ '^[0-5][0-9]$' AND
                    (timezone IS NULL OR timezone ~ '^(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$') AND
                    -- Additional check for days in months (e.g., February)
                    (
                        (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                        (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                        (month = '02' AND day BETWEEN '01' AND
                            CASE
                                -- Handle leap years
                                WHEN (year::int % 400 = 0 OR (year::int % 100 != 0 AND year::int % 4 = 0)) THEN '29'
                                ELSE '28'
                            END
                        )
                    )
                THEN 1
                ELSE 0
            END AS is_valid
         FROM extracted_timestamps
        )
         SELECT COUNT(*) AS valid_count, COUNT(*) AS total_count
         FROM validated_timestamps
         WHERE is_valid = 1;
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
        :return: Tuple containing count of valid timestamps not in the future and total count
        """

        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"{field} ~ '{timestamp_iso_regex}'"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
         WITH extracted_timestamps AS (
         SELECT
            {field},
            SUBSTRING({field} FROM '^(\d{{4}})') AS year,        -- Extract year
            SUBSTRING({field} FROM '^\d{{4}}-(\d{{2}})') AS month, -- Extract month
            SUBSTRING({field} FROM '^\d{{4}}-\d{{2}}-(\d{{2}})') AS day, -- Extract day
            SUBSTRING({field} FROM 'T(\d{{2}})') AS hour,       -- Extract hour
            SUBSTRING({field} FROM 'T\d{{2}}:(\d{{2}})') AS minute, -- Extract minute
            SUBSTRING({field} FROM 'T\d{{2}}:\d{{2}}:(\d{{2}})') AS second, -- Extract second
            SUBSTRING({field} FROM '([+-]\d{{2}}:\d{{2}}|Z)$') AS timezone -- Extract timezone
         FROM {qualified_table_name}
         {filters_clause}
        ),
         validated_timestamps AS (
         SELECT
            {field},
            CASE
                WHEN
                    year ~ '^\d{{4}}$' AND
                    month ~ '^(0[1-9]|1[0-2])$' AND
                    day ~ '^((0[1-9]|[12][0-9])|(30|31))$' AND
                    hour ~ '^([01][0-9]|2[0-3])$' AND
                    minute ~ '^[0-5][0-9]$' AND
                    second ~ '^[0-5][0-9]$' AND
                    (timezone IS NULL OR timezone ~ '^(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])$') AND
                    (
                        (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                        (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                        (month = '02' AND day BETWEEN '01' AND
                            CASE
                                WHEN (year::int % 400 = 0 OR (year::int % 100 != 0 AND year::int % 4 = 0)) THEN '29'
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
         WHERE is_valid = 1 AND ({field} ~ '{timestamp_iso_regex}') AND {field}::timestamp <= CURRENT_TIMESTAMP
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

        timestamp_iso_regex = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.\d{1,3})?(Z|[+-](0[0-9]|1[0-4]):[0-5][0-9])?$"

        if predefined_regex == "timestamp_iso":
            regex_condition = f"{field} ~ '{timestamp_iso_regex}'"
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
         WITH extracted_timestamps AS (
         SELECT
            {field},
            SUBSTRING({field} FROM '^(\d{{4}})') AS year,        -- Extract year
            SUBSTRING({field} FROM '^\d{{4}}-(\d{{2}})') AS month, -- Extract month
            SUBSTRING({field} FROM '^\d{{4}}-\d{{2}}-(\d{{2}})') AS day -- Extract day
         FROM {qualified_table_name}
         {filters_clause}
        ),
         validated_dates AS (
         SELECT
            {field},
            CASE
                WHEN
                    year ~ '^\d{{4}}$' AND
                    month ~ '^(0[1-9]|1[0-2])$' AND
                    day ~ '^((0[1-9]|[12][0-9])|(30|31))$' AND
                    (
                        (month IN ('01', '03', '05', '07', '08', '10', '12') AND day BETWEEN '01' AND '31') OR
                        (month IN ('04', '06', '09', '11') AND day BETWEEN '01' AND '30') OR
                        (month = '02' AND day BETWEEN '01' AND
                            CASE
                                WHEN (year::int % 400 = 0 OR (year::int % 100 != 0 AND year::int % 4 = 0)) THEN '29'
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
         AND ({field} ~ '{timestamp_iso_regex}')
         AND ({field})::date <= CURRENT_DATE  -- Compare only the date part against the current date
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

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0
