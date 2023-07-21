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

from sqlalchemy import URL, create_engine

from datachecks.core.datasource.base import SQLDatasource


class PostgresSQLDatasource(SQLDatasource):
    def __init__(self, data_source_name: str, data_source_properties: Dict):
        super().__init__(data_source_name, data_source_properties)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        url = URL.create(
            drivername="postgresql",
            username=self.data_connection.get("username"),
            password=self.data_connection.get("password"),
            host=self.data_connection.get("host"),
            port=self.data_connection.get("port"),
            database=self.data_connection.get("database"),
        )
        engine = create_engine(url)
        self.connection = engine.connect()
        return self.connection
