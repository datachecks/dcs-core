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

from datachecks.core.common.models.configuration import (
    MetricConfiguration,
    MetricsFilterConfiguration,
)
from datachecks.core.common.models.data_source_resource import Field, Index, Table
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.common.models.validation import Threshold, Validation
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.datasource.search_datasource import SearchIndexDataSource
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

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.DOCUMENT_COUNT,
            resource=Index(name=index_name, data_source=POSTGRES_DATA_SOURCE_NAME),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert isinstance(metric, DocumentCountMetric)
        assert metric.name == "test_document_count_metric"
        assert metric.metric_type == MetricsType.DOCUMENT_COUNT
        assert metric.index_name == "test_index"

    def test_should_create_document_count_metric_with_filter(self):
        mock_datasource = Mock(SearchIndexDataSource)
        mock_datasource.data_source_name.return_value = OPEN_SEARCH_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, index_name = "test_document_count_metric", "test_index"

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.DOCUMENT_COUNT,
            resource=Index(name=index_name, data_source=POSTGRES_DATA_SOURCE_NAME),
            # TODO: Fix this, this needs to be a filter configuration not dict
            filters={"where": '{"range": {"age": {"gte": 30, "lte": 40}}}'},
        )

        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert isinstance(metric, DocumentCountMetric)
        assert metric.name == "test_document_count_metric"
        assert metric.metric_type == MetricsType.DOCUMENT_COUNT
        assert metric.index_name == "test_index"
        assert metric.filter_query == {"range": {"age": {"gte": 30, "lte": 40}}}

    def test_should_create_document_count_metric_with_validation(self):
        mock_datasource = Mock(SearchIndexDataSource)
        mock_datasource.data_source_name.return_value = OPEN_SEARCH_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, index_name = "test_document_count_metric", "test_index"
        validation = Validation(threshold=Threshold(gte=100))

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.DOCUMENT_COUNT,
            resource=Index(name=index_name, data_source=POSTGRES_DATA_SOURCE_NAME),
            validation=validation,
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )
        metric = list(metric_manager.metrics.values())[0]

        assert isinstance(metric, DocumentCountMetric)
        assert metric.name == "test_document_count_metric"
        assert metric.metric_type == MetricsType.DOCUMENT_COUNT
        assert metric.index_name == "test_index"
        assert metric.validation.threshold == Threshold(gte=100)

    def test_should_create_row_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name = "test_row_count_metric", "test_table"

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.ROW_COUNT,
            resource=Table(name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
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

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.ROW_COUNT,
            resource=Table(name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME),
            filters={"where": '{"range": {"age": {"gte": 30, "lte": 40}}}'},
        )

        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert metric.name == metric_name
        assert metric.metric_type == MetricsType.ROW_COUNT
        assert metric.table_name == "test_table"

    def test_should_create_row_count_metric_with_validation(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME
        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name = "test_row_count_metric", "test_table"
        validation = Validation(
            threshold=Threshold(
                gte=100,
            )
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.ROW_COUNT,
            resource=Table(name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME),
            validation=validation,
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )
        metric = list(metric_manager.metrics.values())[0]

        assert metric.name == metric_name
        assert metric.metric_type == MetricsType.ROW_COUNT
        assert metric.table_name == "test_table"
        assert metric.validation.threshold == Threshold(gte=100)

    def test_should_create_max_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = "test_max_metric", "test_table", "age"

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.MAX
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_sum_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = "test_sum_metric", "test_table", "age"

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.SUM,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.SUM
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_distinct_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_distinct_count_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.DISTINCT_COUNT,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.DISTINCT_COUNT
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_variance_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_variance_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.VARIANCE,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.VARIANCE
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_stddev_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_stddev_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.STDDEV,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.STDDEV
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_duplicate_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_duplicate_count_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.DUPLICATE_COUNT,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.DUPLICATE_COUNT
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_null_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_null_count_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.NULL_COUNT,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.NULL_COUNT
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_create_empty_string_count_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_empty_string_count_metric",
            "test_table",
            "description",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.EMPTY_STRING_COUNT,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.EMPTY_STRING_COUNT
            assert metric.table_name == "test_table"
            assert metric.field_name == "description"

    def test_should_create_empty_string_percentage_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource
        metric_name, table_name, field_name = (
            "test_empty_string_percentage_metric",
            "test_table",
            "description",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.EMPTY_STRING_PERCENTAGE,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )

        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]
        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.EMPTY_STRING_PERCENTAGE
            assert metric.table_name == "test_table"
            assert metric.field_name == "description"

    def test_should_create_combined_metric(self):
        datasource_manager = Mock(DataSourceManager)

        metric_name, expression = (
            "test_combined_metric",
            "mul(test_max_metric, test_variance_metric)",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.COMBINED,
            expression=expression,
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.combined.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.COMBINED
            assert metric.expression == expression

    def test_should_create_null_percentage_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name, field_name = (
            "test_null_percentage_metric",
            "test_table",
            "age",
        )

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.NULL_PERCENTAGE,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        if isinstance(metric, FieldMetrics):
            assert metric.name == metric_name
            assert metric.metric_type == MetricsType.NULL_PERCENTAGE
            assert metric.table_name == "test_table"
            assert metric.field_name == "age"

    def test_should_check_validity_of_metric_value(self):
        datasource_manager = Mock(DataSourceManager)

        metric_name, field_name, table_name = (
            "test_max_metric",
            "age",
            "test_table",
        )

        gte_metric_config = MetricConfiguration(
            name=metric_name + "_gte",
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
            validation=Validation(threshold=Threshold(gte=100)),
        )
        lte_metric_config = MetricConfiguration(
            name=metric_name + "_lte",
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
            validation=Validation(threshold=Threshold(lte=100)),
        )
        lt_metric_config = MetricConfiguration(
            name=metric_name + "_lt",
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
            validation=Validation(threshold=Threshold(lt=100)),
        )
        gt_metric_config = MetricConfiguration(
            name=metric_name + "_gt",
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
            validation=Validation(threshold=Threshold(gt=100)),
        )
        eq_metric_config = MetricConfiguration(
            name=metric_name + "_eq",
            metric_type=MetricsType.MAX,
            resource=Field(
                name=field_name,
                belongs_to=Table(
                    name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME
                ),
            ),
            validation=Validation(threshold=Threshold(eq=100)),
        )

        metric_manager = MetricManager(
            metric_config={
                gte_metric_config.name: gte_metric_config,
                lte_metric_config.name: lte_metric_config,
                lt_metric_config.name: lt_metric_config,
                gt_metric_config.name: gt_metric_config,
                eq_metric_config.name: eq_metric_config,
            },
            data_source_manager=datasource_manager,
        )
        gte_metric = list(metric_manager.metrics.values())[0]
        lte_metric = list(metric_manager.metrics.values())[1]
        lt_metric = list(metric_manager.metrics.values())[2]
        gt_metric = list(metric_manager.metrics.values())[3]
        eq_metric = list(metric_manager.metrics.values())[4]

        assert gte_metric.validation.threshold.gte == 100
        assert lte_metric.validation.threshold.lte == 100
        assert lt_metric.validation.threshold.lt == 100
        assert gt_metric.validation.threshold.gt == 100
        assert eq_metric.validation.threshold.eq == 100

        assert gte_metric.validate_metric(101)[0] is True
        assert lte_metric.validate_metric(99)[0] is True
        assert lt_metric.validate_metric(99)[0] is True
        assert gt_metric.validate_metric(101)[0] is True
        assert eq_metric.validate_metric(100)[0] is True

    def test_should_create_custom_sql_metric(self):
        mock_datasource = Mock(DataSource)
        mock_datasource.data_source_name.return_value = POSTGRES_DATA_SOURCE_NAME

        datasource_manager = Mock(DataSourceManager)
        datasource_manager.get_data_source.return_value = mock_datasource

        metric_name, table_name = "test_row_count_metric", "test_table"

        metric_config = MetricConfiguration(
            name=metric_name,
            metric_type=MetricsType.CUSTOM_SQL,
            resource=Table(name=table_name, data_source=POSTGRES_DATA_SOURCE_NAME),
        )
        metric_manager = MetricManager(
            metric_config={metric_name: metric_config},
            data_source_manager=datasource_manager,
        )

        metric = list(metric_manager.metrics.values())[0]

        assert metric.name == "test_row_count_metric"
        assert metric.metric_type == MetricsType.CUSTOM_SQL
        assert metric.table_name == "test_table"
