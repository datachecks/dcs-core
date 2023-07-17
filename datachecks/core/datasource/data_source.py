from abc import ABC, abstractmethod
from dataclasses import asdict
from enum import Enum
from typing import Dict, Any, List

from opensearchpy import OpenSearch
from sqlalchemy import URL, create_engine

from datachecks.core.configuration.configuration import DataSourceConfiguration


class DataSource(ABC):
    """
    Abstract class for data sources
    """
    def __init__(
            self,
            data_source_name: str,
            data_connection: Dict
    ):
        self.data_source_name: str = data_source_name
        self.data_connection: Dict = data_connection

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        raise NotImplementedError("connect method is not implemented")

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        raise NotImplementedError("is_connected method is not implemented")


class SearchIndexDataSource(DataSource):
    """
    Abstract class for search index data sources
    """
    def __init__(
            self,
            data_source_name: str,
            data_connection: Dict
    ):
        super().__init__(data_source_name, data_connection)

        self.client = None

    def query_get_document_count(self, index_name: str, filter: str = None) -> int:
        """
        Get the document count
        :param index_name: name of the index
        :param filter: optional filter
        :return: count of documents
        """
        raise NotImplementedError("query_get_document_count method is not implemented")


class OpenSearchSearchIndexDataSource(SearchIndexDataSource):
    """
    OpenSearch data source
    """
    def __init__(
            self,
            data_source_name: str,
            data_connection: Dict
    ):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> OpenSearch:
        """
        Connect to the data source
        """

        auth = ('admin', 'admin')
        host = self.data_connection.get('host')
        port = int(self.data_connection.get('port'))
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
            ca_certs=False
        )
        return self.client

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.client.ping()

    def query_get_document_count(self, index_name: str, filter: str = None) -> int:
        """
        Get the document count
        :param index_name: name of the index
        :param filter: optional filter
        """
        response = self.client.count(index=index_name, body=filter)

        return response['count']


class SQLDatasource(DataSource):
    """
    Abstract class for SQL data sources
    """
    def __init__(
            self,
            data_source_name: str,
            data_source_properties: Dict
    ):
        super().__init__(data_source_name, data_source_properties)

        self.connection = None

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.connection is not None


class PostgresSQLDatasource(SQLDatasource):

    def __init__(
            self,
            data_source_name: str,
            data_source_properties: Dict
    ):
        super().__init__(data_source_name, data_source_properties)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        url = URL.create(
            drivername="postgresql",
            username=self.data_connection.get('username'),
            password=self.data_connection.get('password'),
            host=self.data_connection.get('host'),
            port=self.data_connection.get('port'),
            database=self.data_connection.get('database')
        )
        engine = create_engine(url)
        self.connection = engine.connect()
        return self.connection


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
            self.data_sources[data_source_config.name] = self.create_data_source(data_source_config=data_source_config)
            self.data_sources[data_source_config.name].connect()

    @staticmethod
    def create_data_source(data_source_config: DataSourceConfiguration) -> DataSource:
        """
        Create a data source
        :param data_source_config: data source configuration
        :return: data source
        """
        if data_source_config.type == 'opensearch':
            return OpenSearchSearchIndexDataSource(
                data_source_name=data_source_config.name,
                data_connection=asdict(data_source_config.connection_config),
            )
        elif data_source_config.type == 'postgres':
            return PostgresSQLDatasource(
                data_source_name=data_source_config.name,
                data_source_properties=asdict(data_source_config.connection_config)
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
