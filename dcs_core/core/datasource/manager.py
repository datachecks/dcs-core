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
import importlib
from dataclasses import asdict
from typing import Dict, List

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.configuration import (
    Configuration,
    DataSourceConfiguration,
)
from dcs_core.core.datasource.base import DataSource


class DataSourceManager:
    """
    Data source manager.
    This class is responsible for managing the data sources.

    """

    DATA_SOURCE_CLASS_NAME_MAPPER = {
        "opensearch": "OpenSearchDataSource",
        "elasticsearch": "ElasticSearchDataSource",
        "postgres": "PostgresDataSource",
        "mysql": "MysqlDataSource",
        "bigquery": "BigQueryDataSource",
        "databricks": "DatabricksDataSource",
        "redshift": "RedShiftDataSource",
        "snowflake": "SnowFlakeDataSource",
        "mssql": "MssqlDataSource",
        "oracle": "OracleDataSource",
        "db2": "DB2DataSource",
        "sybase": "SybaseDataSource",
    }

    def __init__(self, config: Configuration):
        self._config = config
        self._data_sources: Dict[str, DataSource] = {}

    def connect(self):
        for name, data_source_config in self._config.data_sources.items():
            self._data_sources[data_source_config.name] = self._create_data_source(
                data_source_config=data_source_config
            )
        for data_source in self._data_sources.values():
            try:
                data_source.connect()
            except Exception as e:
                raise DataChecksDataSourcesConnectionError(
                    f"Failed to connect to data source {data_source.data_source_name} [{str(e)}]"
                )

    @property
    def get_data_sources(self) -> Dict[str, DataSource]:
        """
        Get the data sources
        :return:
        """
        return self._data_sources

    def _create_data_source(
        self, data_source_config: DataSourceConfiguration
    ) -> DataSource:
        """
        Create a data source
        :param data_source_config: data source configuration
        :return: data source
        """
        data_source_name = data_source_config.name
        data_source_type = data_source_config.type
        if data_source_type == "spark_df":
            from dcs_core.integrations.databases.spark_df import SparkDFDataSource

            return SparkDFDataSource(
                data_source_name,
                {"spark_session": data_source_config.connection_config.spark_session},
            )
        try:
            module_name = (
                f"dcs_core.integrations.databases.{data_source_config.type.value}"
            )
            module = importlib.import_module(module_name)
            data_source_class = self.DATA_SOURCE_CLASS_NAME_MAPPER[
                data_source_config.type
            ]
            data_source_class = getattr(module, data_source_class)
            return data_source_class(
                data_source_name, asdict(data_source_config.connection_config)
            )
        except ModuleNotFoundError as e:
            raise DataChecksDataSourcesConnectionError(
                f'Failed to initiate data source type "{data_source_type}" [{str(e)}]'
            )

    def get_data_source(self, data_source_name: str) -> DataSource:
        """
        Get a data source
        :param data_source_name:
        :return:
        """
        return self._data_sources[data_source_name]

    def get_data_source_names(self) -> List[str]:
        """
        Get the data source names
        :return:
        """
        return list(self._data_sources.keys())
