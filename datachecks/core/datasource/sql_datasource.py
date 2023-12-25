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
from typing import Dict, List, Union

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from datachecks.core.datasource.base import DataSource


class SQLDataSource(DataSource):
    """
    Abstract class for SQL data sources
    """

    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

        self.connection: Union[Connection, None] = None
        self.database: str = data_connection.get("database")

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.connection is not None

    def close(self):
        self.connection.close()

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        return f"{table_name}"

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
        return inspect(self.connection.engine).get_table_names(schema="public")

    def query_get_row_count(self, table: str, filters: str = None) -> int:
        """
        Get the row count
        :param table: name of the table
        :param filters: optional filter
        """
        qualified_table_name = self.qualified_table_name(table)
        query = f"SELECT COUNT(*) FROM {qualified_table_name} AS row_count"
        if filters:
            query += f" WHERE {filters}"

        return self.connection.execute(text(query)).fetchone()[0]

    def query_get_custom_sql(self, query: str) -> Union[int, float, None]:
        """
        Get the first row of the custom sql query
        :param query: custom sql query
        """
        row = self.connection.execute(text(query)).fetchone()
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

        query = "SELECT MAX({}) FROM {}".format(field, qualified_table_name)

        if filters:
            query += " WHERE {}".format(filters)
        var = self.connection.execute(text(query)).fetchone()[0]
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
        query = "SELECT MIN({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return self.connection.execute(text(query)).fetchone()[0]

    def query_get_avg(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the average value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT AVG({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.connection.execute(text(query)).fetchone()[0], 2)

    def query_get_sum(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the sum value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT SUM({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.connection.execute(text(query)).fetchone()[0], 2)

    def query_get_variance(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the variance value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT VAR_SAMP({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.connection.execute(text(query)).fetchone()[0], 2)

    def query_get_stddev(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the standard deviation value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT STDDEV_SAMP({}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return round(self.connection.execute(text(query)).fetchone()[0], 2)

    def query_get_null_count(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the null count
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        qualified_table_name = self.qualified_table_name(table)
        query = "SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(
            qualified_table_name, field
        )
        if filters:
            query += " AND {}".format(filters)
        return self.connection.execute(text(query)).fetchone()[0]

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
        query = "SELECT COUNT(*) FROM {} WHERE {} = ''".format(
            qualified_table_name, field
        )
        if filters:
            query += " AND {}".format(filters)
        result = self.connection.execute(text(query)).fetchone()
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
        query = "SELECT SUM(CASE WHEN {} = '' THEN 1 ELSE 0 END) AS empty_string_count, COUNT(*) AS total_count FROM {}".format(
            field, qualified_table_name
        )

        if filters:
            query += " WHERE {}".format(filters)

        result = self.connection.execute(text(query)).fetchone()
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
        query = "SELECT COUNT(DISTINCT {}) FROM {}".format(field, qualified_table_name)
        if filters:
            query += " WHERE {}".format(filters)

        return self.connection.execute(text(query)).fetchone()[0]

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
        query = "SELECT SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS null_count, COUNT(*) AS total_count FROM {}".format(
            field, qualified_table_name
        )

        if filters:
            query += " WHERE {}".format(filters)

        result = self.connection.execute(text(query)).fetchone()
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
        query = f"""
            SELECT {field} from {qualified_table_name} ORDER BY {field} DESC LIMIT 1;
        """
        result = self.connection.execute(text(query)).fetchone()
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

        result = self.connection.execute(text(query)).fetchone()
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

        result = self.connection.execute(text(query)).fetchone()
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
        query = f"""
            SELECT
            count(*) as duplicate_count
            FROM {qualified_table_name}
            {filters}
            GROUP BY {field}
            HAVING COUNT(*) > 1
            """

        result = self.connection.execute(text(query)).fetchall()
        return len(result) if result else 0
