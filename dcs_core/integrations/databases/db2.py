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

from dcs_core.integrations.utils.utils import ibm_db2_dll_files_loader

ibm_db2_dll_files_loader()
from typing import Any, Dict

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class DB2DataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> Any:
        """
        Connect to the DB2 data source using SQLAlchemy
        """
        try:
            url = self._build_connection_url()
            engine = create_engine(url, echo=False)
            self.connection = engine.connect()
            return self.connection
        except SQLAlchemyError as e:
            raise DataChecksDataSourcesConnectionError(
                f"Failed to connect to DB2 data source: {str(e)}"
            )

    def _build_connection_url(self) -> str:
        """
        Build the SQLAlchemy connection URL for DB2
        """
        host = self.data_connection.get("host")
        port = self.data_connection.get("port")
        database = self.data_connection.get("database")
        username = self.data_connection.get("username")
        password = self.data_connection.get("password")

        url = f"db2+ibm_db://{username}:{password}@{host}:{port}/{database}"

        params = []
        if self.data_connection.get("security"):
            params.append(f"SECURITY={self.data_connection['security']}")
        if self.data_connection.get("protocol"):
            params.append(f"PROTOCOL={self.data_connection['protocol']}")
        if self.data_connection.get("schema"):
            params.append(f"CURRENTSCHEMA={self.data_connection.get('schema')}")
        if params:
            url += "?" + "&".join(params)

        return url
