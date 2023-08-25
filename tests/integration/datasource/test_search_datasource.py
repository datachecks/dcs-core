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

import datetime

import pytest
from opensearchpy import OpenSearch

from datachecks.core.common.models.configuration import (
    DataSourceConnectionConfiguration,
)
from datachecks.integrations.databases.opensearch import OpenSearchDataSource
from tests.utils import create_opensearch_client

INDEX_NAME = "reliability_metric_test"


def populate_opensearch_datasource(opensearch_client: OpenSearch):
    try:
        opensearch_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
        opensearch_client.indices.create(
            index=INDEX_NAME,
            body={"mappings": {"properties": {"last_fight": {"type": "date"}}}},
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "thor",
                "age": 1500,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=10),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "captain america",
                "age": 100,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=3),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "iron man",
                "age": 50,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=4),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "hawk eye",
                "age": 40,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=5),
            },
        )
        opensearch_client.index(
            index=INDEX_NAME,
            body={
                "name": "black widow",
                "age": 35,
                "last_fight": datetime.datetime.utcnow() - datetime.timedelta(days=6),
            },
        )
        opensearch_client.indices.refresh(index=INDEX_NAME)
    except Exception as e:
        print(e)


@pytest.fixture(scope="class")
@pytest.mark.usefixtures("opensearch_client_configuration")
def setup_data(
    opensearch_client_configuration: DataSourceConnectionConfiguration,
):
    opensearch_client = create_opensearch_client(opensearch_client_configuration)
    try:
        populate_opensearch_datasource(opensearch_client)
        yield True
    except Exception as e:
        print(e)
    finally:
        opensearch_client.indices.delete(index="numeric_metric_test", ignore=[400, 404])

        opensearch_client.close()


@pytest.mark.usefixtures("setup_data", "opensearch_datasource")
class TestSQLDatasourceQueries:
    def test_should_return_avg_with_filter(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        avg = opensearch_datasource.query_get_avg(
            INDEX_NAME, "age", {"match": {"name": "thor"}}
        )
        assert avg == 1500

    def test_should_return_min_with_filter(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        min_value_age = opensearch_datasource.query_get_min(
            INDEX_NAME, "age", {"match_all": {}}
        )
        assert min_value_age == 35

    def test_should_return_max_with_filter(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        max_value_age = opensearch_datasource.query_get_max(
            INDEX_NAME, "age", {"match_all": {}}
        )
        assert max_value_age == 1500

    def test_should_return_document_count_with_filter(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        count = opensearch_datasource.query_get_document_count(
            INDEX_NAME, {"match_all": {}}
        )
        assert count == 5

    def test_should_calculate_time_diff_in_second(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        diff = opensearch_datasource.query_get_time_diff(INDEX_NAME, "last_fight")
        assert diff >= 24 * 3600 * 3

    def test_index_field_metadata(self, opensearch_datasource: OpenSearchDataSource):
        index_field_metadata = opensearch_datasource.query_get_field_metadata(
            INDEX_NAME
        )

        assert index_field_metadata["name"] == str
        assert index_field_metadata["age"] == int
        assert index_field_metadata["last_fight"] == datetime.datetime

    def test_index_metadata(self, opensearch_datasource: OpenSearchDataSource):
        indices = opensearch_datasource.query_get_index_metadata()

        assert INDEX_NAME in indices

    def test_should_return_numeric_profile(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        profile = opensearch_datasource.profiling_search_aggregates_numeric(
            INDEX_NAME, "age"
        )

        assert profile["min"] == 35
        assert profile["max"] == 1500
        assert profile["avg"] == 345
        assert profile["sum"] == 1725
        assert profile["distinct_count"] == 5
        assert profile["missing_count"] == 0

    def test_should_return_text_profile(
        self, opensearch_datasource: OpenSearchDataSource
    ):
        profile = opensearch_datasource.profiling_search_aggregates_string(
            INDEX_NAME, "name"
        )
        assert profile["distinct_count"] == 5
        assert profile["missing_count"] == 0
        assert profile["max_length"] == 15
        assert profile["min_length"] == 4
        assert profile["avg_length"] == 9.2
