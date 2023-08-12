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

from datachecks.core.datasource.base import DataSource


class SearchIndexDataSource(DataSource):
    """
    Abstract class for search index data sources
    """

    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

        self.client = None

    def query_get_document_count(self, index_name: str, filters: Dict = None) -> int:
        """
        Get the document count
        :param index_name: name of the index
        :param filters: optional filter
        :return: count of documents
        """
        body = {"query": filters} if filters else {}
        response = self.client.count(index=index_name, body=body)
        return response["count"]

    def query_get_max(self, index_name: str, field: str, filters: str = None) -> int:
        """
        Get the max value
        :param index_name: name of the index
        :param field: field name
        :param filters: optional filter
        :return: max value
        """
        query = {"aggs": {"max_value": {"max": {"field": field}}}}
        if filters:
            query["query"] = filters

        response = self.client.search(index=index_name, body=query)
        return response["aggregations"]["max_value"]["value"]

    def query_get_min(self, index_name: str, field: str, filters: Dict = None) -> int:
        """
        Get the min value of a field
        :param index_name:
        :param field:
        :param filters:
        :return:
        """
        query = {"aggs": {"min_value": {"min": {"field": field}}}}
        if filters:
            query["query"] = filters

        response = self.client.search(index=index_name, body=query)
        return response["aggregations"]["min_value"]["value"]

    def query_get_avg(self, index_name: str, field: str, filters: Dict = None) -> int:
        """
        Get the average value of a field
        :param index_name:
        :param field:
        :param filters:
        :return:
        """
        query = {"aggs": {"avg_value": {"avg": {"field": field}}}}
        if filters:
            query["query"] = filters

        response = self.client.search(index=index_name, body=query)
        return round(response["aggregations"]["avg_value"]["value"], 2)

    def query_get_time_diff(self, index_name: str, field: str) -> int:
        """
        Get the time difference
        :param index_name: name of the index
        :param field: field name
        :param filters: optional filter
        :return: time difference in milliseconds
        """
        query = {"query": {"match_all": {}}, "sort": [{f"{field}": {"order": "desc"}}]}

        response = self.client.search(index=index_name, body=query)

        if response["hits"]["hits"]:
            last_updated = response["hits"]["hits"][0]["_source"][field]

            last_updated = parser.parse(timestr=last_updated).timestamp()
            now = datetime.utcnow().timestamp()
            return int(now - last_updated)

        return 0
