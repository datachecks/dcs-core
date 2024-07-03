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

from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from datachecks.core.common.errors import DataChecksDataSourcesConnectionError
from datachecks.core.datasource.sql_datasource import SQLDataSource


class MssqlDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            url = URL.create(
                drivername="mssql+pyodbc",
                username=self.data_connection.get("username"),
                password=self.data_connection.get("password"),
                host=self.data_connection.get("host"),
                port=self.data_connection.get("port", 1433),
                database=self.data_connection.get("database"),
            )
            schema = self.data_connection.get("schema") or "dbo"
            # For osx have to install
            # brew install unixodbc
            # brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
            # brew update
            # brew install msodbcsql mssql-tools
            driver = self.data_connection.get("driver", "ODBC Driver 17 for SQL Server")
            url_updated = f"{url}?driver={driver}"
            engine = create_engine(
                url=url_updated,
                connect_args={"options": f"-csearch_path={schema}"},
                isolation_level="AUTOCOMMIT",
            )
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Mssql data source: [{str(e)}]"
            )
