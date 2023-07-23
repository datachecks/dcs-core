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
from datetime import datetime
from typing import Dict

from dateutil import parser
from opensearchpy import OpenSearch

from datachecks.core.datasource.base import SearchIndexDataSource


class OpenSearchSearchIndexDataSource(SearchIndexDataSource):
    """
    OpenSearch data source
    """

    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

    def connect(self) -> OpenSearch:
        """
        Connect to the data source
        """

        auth = (
            self.data_connection.get("username"),
            self.data_connection.get("password"),
        )
        host = self.data_connection.get("host")
        port = int(self.data_connection.get("port"))
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
            ca_certs=False,
        )
        return self.client

    def close(self):
        """
        Close the connection
        """
        self.client.close()

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        return self.client.ping()

    def query_get_document_count(self, index_name: str, filters: Dict = None) -> int:
        """
        Get the document count
        :param index_name: name of the index
        :param filters: optional filter
        """
        body = {"query": filters} if filters else {}
        response = self.client.count(index=index_name, body=body)
        return response["count"]

    def query_get_max(self, index_name: str, field: str, filters: Dict = None) -> int:
        """
        Get the max value of a field
        :param index_name:
        :param field:
        :param filters:
        :return:
        """
        query = {"aggs": {"max_value": {"max": {"field": field}}}}
        if filters:
            query["query"] = filters

        response = self.client.search(index=index_name, body=query)
        return response["aggregations"]["max_value"]["value"]

    def query_get_time_diff(self, index_name: str, field: str) -> int:
        """
        Get the time difference between the latest and the now
        :param index_name:
        :param field:
        :return:
        """
        query = {"query": {"match_all": {}}, "sort": [{f"{field}": {"order": "desc"}}]}

        response = self.client.search(index=index_name, body=query)

        if response["hits"]["hits"]:
            last_updated = response["hits"]["hits"][0]["_source"][field]

            last_updated = parser.parse(timestr=last_updated).timestamp()
            now = datetime.utcnow().timestamp()
            return int(now - last_updated)

        return 0
