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

import pytest
from opensearchpy import OpenSearch

from datachecks.core.datasource.opensearch import \
    OpenSearchSearchIndexDataSource
from datachecks.core.metric.base import MetricsType
from datachecks.core.metric.numeric_metric import DocumentCountMetric


@pytest.mark.usefixtures("opensearch_client")
@pytest.fixture(scope="module")
def setup_data(opensearch_client: OpenSearch):
    opensearch_client.indices.delete(index="test", ignore=[400, 404])
    opensearch_client.indices.create(index="test")
    opensearch_client.index(index="test", body={"test": "test"})
    opensearch_client.indices.refresh(index="test")


@pytest.mark.usefixtures("opensearch_datasource", "setup_data")
class TestDocumentCountMetric:
    def test_document_count_metric(
        self, opensearch_datasource: OpenSearchSearchIndexDataSource
    ):
        doc = DocumentCountMetric(
            name="test",
            data_source=opensearch_datasource,
            index_name="test",
            metric_type=MetricsType.DOCUMENT_COUNT,
        )
        doc_value = doc.get_value()
        assert doc_value["value"] == 1
