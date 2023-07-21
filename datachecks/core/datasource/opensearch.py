from typing import Dict

from opensearchpy import OpenSearch

from datachecks.core.datasource.base import SearchIndexDataSource


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

        auth = (self.data_connection.get('username'), self.data_connection.get('password'))
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