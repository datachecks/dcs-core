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

from datachecks.core.common.models.metric import MetricsType
from datachecks.core.configuration.configuration import (
    MetricConfiguration,
    MetricsFilterConfiguration,
)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.metric.base import FieldMetrics
from datachecks.core.metric.manager import MetricManager
from datachecks.core.metric.reliability_metric import DocumentCountMetric

OPEN_SEARCH_DATA_SOURCE_NAME = "test_open_search_data_source"
POSTGRES_DATA_SOURCE_NAME = "test_postgres_data_source"


class TestMetricManager:
    def test_should_create_document_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = OPEN_SEARCH_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, index_name = "test_document_count_metric", "test_index"
        metric_config = {
            "metric_type": "document_count",
            "name": metric_name,
            "index": index_name,
        }

        metric_config = MetricConfiguration(**metric_config)
        metric_manager = MetricManager(
            metric_config={OPEN_SEARCH_DATA_SOURCE_NAME: [metric_config]},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert isinstance(metric, DocumentCountMetric)
        assert metric.name == "test_document_count_metric"
        assert metric.metric_type == MetricsType.DOCUMENT_COUNT
        assert metric.index_name == "test_index"

    def test_should_create_document_count_metric_with_filter(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = OPEN_SEARCH_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, index_name = "test_document_count_metric", "test_index"

        metric_config = {
            "metric_type": "document_count",
            "name": metric_name,
            "index": index_name,
        }
        filters = {"search_query": '{"range": {"age": {"gte": 30, "lte": 40}}}'}

        metric_config = MetricConfiguration(**metric_config)
        metric_config.filters = MetricsFilterConfiguration(**filters)
        metric_manager = MetricManager(
            metric_config={OPEN_SEARCH_DATA_SOURCE_NAME: [metric_config]},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert isinstance(metric, DocumentCountMetric)
        assert metric.name == "test_document_count_metric"
        assert metric.metric_type == MetricsType.DOCUMENT_COUNT
        assert metric.index_name == "test_index"
        assert metric.filter_query == {"range": {"age": {"gte": 30, "lte": 40}}}

    def test_should_create_row_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name = "test_row_count_metric", "test_table"
        metric_config = {
            "metric_type": "row_count",
            "name": metric_name,
            "table": table_name,
        }

        metric_config = MetricConfiguration(**metric_config)
        metric_manager = MetricManager(
            metric_config={POSTGRES_DATA_SOURCE_NAME: [metric_config]},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert metric.name == "test_row_count_metric"
        assert metric.metric_type == MetricsType.ROW_COUNT
        assert metric.table_name == "test_table"

    def test_should_create_row_count_metric_with_filters(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name = "test_row_count_metric", "test_table"
        metric_config = {
            "metric_type": "row_count",
            "name": metric_name,
            "table": table_name,
        }
        filters = {"where_clause": "age > 30"}
        metric_config = MetricConfiguration(**metric_config)
        metric_config.filters = MetricsFilterConfiguration(**filters)
        metric_manager = MetricManager(
            metric_config={POSTGRES_DATA_SOURCE_NAME: [metric_config]},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert metric.name == metric_name
        assert metric.metric_type == MetricsType.ROW_COUNT
        assert metric.table_name == "test_table"

    def test_should_create_max_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = "test_max_metric", "test_table", "age"
        metric_config = {
            "metric_type": "max",
            "name": metric_name,
            "table": table_name,
            "field": field_name,
        }

        metric_config = MetricConfiguration(**metric_config)
        metric_manager = MetricManager(
            metric_config={POSTGRES_DATA_SOURCE_NAME: [metric_config]},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.MAX
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"
