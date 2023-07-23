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

from dataclasses import asdict
from typing import Dict, List

from datachecks.core.configuration.configuration import (
    DataSourceConfiguration, DatasourceType)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.opensearch import \
    OpenSearchSearchIndexDataSource
from datachecks.core.datasource.postgres import PostgresSQLDatasource


class DataSourceManager:
    """
    Data source manager.
    This class is responsible for managing the data sources.

    """

    def __init__(self, config: List[DataSourceConfiguration]):
        self.data_source_configs: List[DataSourceConfiguration] = config
        self.data_sources: Dict[str, DataSource] = {}
        self._initialize_data_sources()

    def _initialize_data_sources(self):
        """
        Initialize the data sources
        :return:
        """
        for data_source_config in self.data_source_configs:
            self.data_sources[data_source_config.name] = self._create_data_source(
                data_source_config=data_source_config
            )
            self.data_sources[data_source_config.name].connect()

    @staticmethod
    def _create_data_source(data_source_config: DataSourceConfiguration) -> DataSource:
        """
        Create a data source
        :param data_source_config: data source configuration
        :return: data source
        """
        if data_source_config.type == DatasourceType.OPENSEARCH:
            return OpenSearchSearchIndexDataSource(
                data_source_name=data_source_config.name,
                data_connection=asdict(data_source_config.connection_config),
            )
        elif data_source_config.type == DatasourceType.POSTGRES:
            return PostgresSQLDatasource(
                data_source_name=data_source_config.name,
                data_source_properties=asdict(data_source_config.connection_config),
            )
        else:
            raise ValueError(f"Unsupported data source type: {data_source_config.type}")

    def get_data_source(self, data_source_name: str) -> DataSource:
        """
        Get a data source
        :param data_source_name:
        :return:
        """
        return self.data_sources[data_source_name]

    def get_data_source_names(self) -> List[str]:
        """
        Get the data source names
        :return:
        """
        return list(self.data_sources.keys())
