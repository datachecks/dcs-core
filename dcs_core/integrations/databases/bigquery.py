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

import base64
import json
import os
from typing import Any, Dict, List

from loguru import logger
from sqlalchemy import create_engine

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.data_source_resource import RawColumnInfo
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class BigQueryDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.project_id = self.data_connection.get("project")
        self.dataset_id = self.data_connection.get("dataset")
        self.schema_name = self.dataset_id
        self.keyfile = self.data_connection.get("keyfile")
        self.credentials_base64 = self.data_connection.get("credentials_base64")

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            credentials = None
            if self.credentials_base64:
                credentials = self.credentials_base64
            elif self.keyfile:
                if os.path.exists(self.keyfile):
                    with open(self.keyfile, "rb") as f:
                        credentials = f.read()
                    credentials = json.loads(credentials)
                    credentials = base64.b64encode(
                        json.dumps(credentials).encode("utf-8")
                    ).decode("utf-8")
                else:
                    try:
                        if self._is_base64(self.keyfile):
                            credentials = self.keyfile
                        else:
                            credentials = base64.b64decode(self.keyfile).decode("utf-8")
                    except Exception as e:
                        logger.error(f"Failed to decode keyfile: {e}")
                        credentials = json.loads(self.keyfile)
                        credentials = base64.b64encode(
                            json.dumps(credentials).encode("utf-8")
                        ).decode("utf-8")

            if not credentials:
                raise
            url = f"bigquery://{self.project_id}/{self.dataset_id}"
            engine = create_engine(url, credentials_base64=credentials)
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to BigQuery data source: [{str(e)}]"
            )

    def _is_base64(self, s: str) -> bool:
        try:
            if len(s) % 4 != 0:
                return False
            base64.b64decode(s, validate=True)
            return True
        except Exception:
            return False

    def quote_column(self, column: str) -> str:
        """
        Quote the column name
        :param column: name of the column
        :return: quoted column name
        """
        return f"`{column}`"

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        if self.project_id and self.dataset_id:
            return f"`{self.project_id}`.`{self.dataset_id}`.`{table_name}`"
        elif self.dataset_id:
            return f"`{self.dataset_id}`.`{table_name}`"
        elif self.project_id:
            return f"`{self.project_id}`.`{table_name}`"

        return f"`{table_name}`"

    def query_get_table_names(self, schema: str | None = None) -> List[str]:
        """
        Get the list of BigQuery tables (excluding views) in a dataset.
        :param schema: optional dataset name
        :return: list of table names
        """
        schema = schema or self.schema_name
        project = self.project_id
        query = (
            f"SELECT table_name FROM `{project}.{schema}.INFORMATION_SCHEMA.TABLES` "
            "WHERE table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        rows = self.fetchall(query)
        return [row[0] for row in rows] if rows else []

    def query_get_table_columns(
        self,
        table: str,
        schema: str | None = None,
    ) -> RawColumnInfo:
        """
        Get the list of tables in the database.
        :param schema: optional schema name
        :return: list of table names
        """
        schema = schema or self.schema_name
        query = (
            "SELECT column_name, data_type, "
            "NULL AS datetime_precision, "
            "NULL AS numeric_precision, "
            "NULL AS numeric_scale, "
            "NULL AS collation_name, "
            "NULL AS character_maximum_length "
            f"FROM `{self.project_id}.{schema}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = '{table}'"
        )

        rows = self.fetchall(query)
        if not rows:
            raise RuntimeError(
                f"{table}: Table, {schema}: Schema, does not exist, or has no columns"
            )
        column_info = {
            r[0]: RawColumnInfo(
                column_name=self.safe_get(r, 0),
                data_type=self.safe_get(r, 1),
                datetime_precision=self.safe_get(r, 2),
                numeric_precision=self.safe_get(r, 3),
                numeric_scale=self.safe_get(r, 4),
                collation_name=self.safe_get(r, 5),
                character_maximum_length=self.safe_get(r, 6),
            )
            for r in rows
        }
        return column_info
