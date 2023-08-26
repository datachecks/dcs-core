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

from unittest.mock import Mock

import pytest

from datachecks.core.common.errors import DataChecksMetricGenerationError
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.datasource.search_datasource import SearchIndexDataSource
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.reliability_metric import (
    DocumentCountMetric,
    FreshnessValueMetric,
    RowCountMetric,
)

INDEX_NAME = "test_index"


class TestDocumentCountMetric:
    def test_should_return_document_count_metric_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_document_count.return_value = 151

        doc = DocumentCountMetric(
            name="document_count_metric_test_2",
            data_source=mock_data_source,
            index_name=INDEX_NAME,
            metric_type=MetricsType.DOCUMENT_COUNT,
        )
        doc_value = doc.get_metric_value()
        assert doc_value.value == 151

    def test_should_return_document_count_metric_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_document_count.return_value = 51

        doc = DocumentCountMetric(
            name="document_count_metric_test_2",
            data_source=mock_data_source,
            index_name=INDEX_NAME,
            metric_type=MetricsType.DOCUMENT_COUNT,
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 40}}}'},
        )
        doc_value = doc.get_metric_value()
        assert doc_value.value == 51


class TestRowCountMetric:
    def test_should_get_row_count_metric_from_sql_datasource_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_row_count.return_value = 10

        row_count_metric = RowCountMetric(
            name="row_count_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.ROW_COUNT,
            table_name="test_table",
            filters={"where_clause": "age >= 30 AND age <= 40"},
        )
        metric_value = row_count_metric.get_metric_value()
        assert metric_value.value == 10
        assert metric_value.metric_type == MetricsType.ROW_COUNT
        assert metric_value.timestamp is not None

    def test_should_get_row_count_metric_from_sql_datasource_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_row_count.return_value = 10

        row_count_metric = RowCountMetric(
            name="row_count_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.ROW_COUNT,
            table_name="test_table",
        )
        metric_value = row_count_metric.get_metric_value()
        assert metric_value.value == 10
        assert metric_value.metric_type == MetricsType.ROW_COUNT
        assert metric_value.timestamp is not None

    def test_should_return_none_wrong_datasource_provided(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"

        row_count_metric = RowCountMetric(
            name="row_count_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.ROW_COUNT,
            table_name="test_table",
            filters={"where_clause": "age >= 30 AND age <= 40"},
        )
        assert row_count_metric.get_metric_value() is None


class TestFreshnessValueMetric:
    def test_should_get_freshness_value_from_sql_datasource(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_time_diff.return_value = 3600

        freshness_metric = FreshnessValueMetric(
            name="freshness_value_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.FRESHNESS,
            table_name="test_table",
            field_name="test_field",
        )
        metric_value = freshness_metric.get_metric_value()
        assert metric_value.value == 3600
        assert metric_value.timestamp is not None

    def test_should_get_freshness_value_from_search_index_datasource(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_time_diff.return_value = 3601

        freshness_metric = FreshnessValueMetric(
            name="freshness_value_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.FRESHNESS,
            index_name="test_index",
            field_name="test_field",
        )
        metric_value = freshness_metric.get_metric_value()
        assert metric_value.value == 3601
        assert metric_value.timestamp is not None

    def test_should_return_none_if_wrong_datasource_type(self):
        mock_data_source = Mock(spec=str)
        mock_data_source.data_source_name = "test_data_source"

        freshness_metric = FreshnessValueMetric(
            name="freshness_value_metric_test",
            data_source=mock_data_source,
            metric_type=MetricsType.FRESHNESS,
            index_name="test_index",
            field_name="test_field",
        )
        assert freshness_metric.get_metric_value() is None
