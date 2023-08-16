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

from sqlalchemy import Connection, Row, inspect, text

from datachecks.core.datasource.base import DataSource


class SQLDatasource(DataSource):
    """
    Abstract class for SQL data sources
    """

    def __init__(self, data_source_name: str, data_source_properties: Dict):
        super().__init__(data_source_name, data_source_properties)

        self.connection: Union[Connection, None] = None
        self.database: str = data_source_properties.get("database")

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.connection is not None

    def close(self):
        self.connection.close()

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
        query = f"SELECT COUNT(*) FROM {table} AS row_count"
        if filters:
            query += f" WHERE {filters}"

        return self.connection.execute(text(query)).fetchone()[0]

    def query_get_max(self, table: str, field: str, filters: str = None) -> int:
        """
        Get the max value
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return:
        """
        query = "SELECT MAX({}) FROM {}".format(field, table)

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
        query = "SELECT MIN({}) FROM {}".format(field, table)
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
        query = "SELECT ROUND(AVG({}), 2) FROM {}".format(field, table)
        if filters:
            query += " WHERE {}".format(filters)

        return self.connection.execute(text(query)).fetchone()[0]

    def query_get_time_diff(self, table: str, field: str) -> int:
        """
        Get the time difference
        :param table: name of the index
        :param field: field name of updated time column
        :return: time difference in seconds
        """
        query = f"""
            SELECT {field} from {table} ORDER BY {field} DESC LIMIT 1;
        """
        result = self.connection.execute(text(query)).fetchone()
        if result:
            return int((datetime.utcnow() - result[0]).total_seconds())
        return 0

    def profiling_sql_aggregates_numeric(
        self, table_name: str, column_name: str
    ) -> Dict:
        column_name = f'"{column_name}"'
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
            FROM {table_name}
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
        query = f"""
            SELECT
                count(distinct({column_name})) as distinct_count,
                sum(case when {column_name} is null then 1 else 0 end) as missing_count,
                max(length({column_name})) as max_length,
                min(length({column_name})) as min_length,
                avg(length({column_name})) as avg_length
            FROM {table_name}
            """

        result = self.connection.execute(text(query)).fetchone()
        return {
            "distinct_count": result[0],
            "missing_count": result[1],
            "max_length": result[2],
            "min_length": result[3],
            "avg_length": result[4],
        }
