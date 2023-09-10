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

from datachecks.core.common.errors import DataChecksDataSourcesConnectionError
from datachecks.core.datasource.sql_datasource import SQLDataSource


class BigQueryDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.project_id = self.data_connection.get("project")
        self.dataset_id = self.data_connection.get("dataset")

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            credentials_base64 = self.data_connection.get("credentials_base64")

            url = f"bigquery://{self.project_id}/{self.dataset_id}"
            engine = create_engine(url, credentials_base64=credentials_base64)
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to BigQuery data source: [{str(e)}]"
            )

    # `retail-search-e6ba`.eventsdataset.wordcount_output

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        return f"`{self.project_id}`.{self.dataset_id}.{table_name}"
