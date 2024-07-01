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
import urllib.parse
from typing import Any, Dict

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from datachecks.core.common.errors import DataChecksDataSourcesConnectionError
from datachecks.core.datasource.sql_datasource import SQLDataSource


class SnowFlakeDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            url = URL(
                account=self.data_connection.get("account"),
                user=self.data_connection.get("username"),
                password=urllib.parse.quote(self.data_connection.get("password")),
                database=self.data_connection.get("database"),
                schema=self.data_connection.get("schema"),
                warehouse=self.data_connection.get("warehouse"),
                role=self.data_connection.get("role"),
            )
            engine = create_engine(url)
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Snowflake data source: [{str(e)}]"
            )
