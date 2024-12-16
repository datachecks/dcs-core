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

import re
from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class SybaseDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the Sybase data source
        """
        try:
            driver = self.data_connection.get("driver") or "Adaptive Server Enterprise"
            if not driver.startswith("{") and not driver.endswith("}"):
                driver = "{" + driver + "}"

            schema = self.data_connection.get("schema") or "dbo"
            engine = create_engine(
                f"sybase+pyodbc://:@",
                connect_args={
                    "DRIVER": driver,
                    "UID": self.data_connection.get("username"),
                    "PWD": self.data_connection.get("password"),
                    "SERVER": self.data_connection.get("host"),
                    "PORT": self.data_connection.get("port"),
                    "DATABASE": self.data_connection.get("database"),
                    "options": f"-csearch_path={schema}",
                },
                isolation_level="AUTOCOMMIT",
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
