from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any

from opensearchpy import OpenSearch


class DataSource(ABC):
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


class SearchIndexDataSource(ABC, DataSource):

    def __init__(
            self,
            data_source_name: str,
            data_connection: Dict
    ):
        super().__init__(data_source_name, data_connection)

        self.client = None

    def query_get_document_count(self, index_name: str, filter: str = None) -> int:
        raise NotImplementedError("query_get_document_count method is not implemented")


class OpenSearchSearchIndexDataSource(SearchIndexDataSource):

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
        ca_certs_path = '/full/path/to/root-ca.pem'
        self.client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=auth,
        )
        return self.client

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.client.ping()

    def query_get_document_count(self, index_name: str, filter: str) -> int:
        """
        Get the document count
        """
        response = self.client.count(index=index_name, body=filter)

        return response['count']


class SQLDatasource(ABC, DataSource):

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


class DataSourceManager:

    def __init__(self):
        self.data_sources: Dict[str, DataSource] = {}

