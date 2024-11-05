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

from typing import Any, Dict, Union

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.integrations.databases.db2 import DB2DataSource


class MysqlDataSource(DB2DataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

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
