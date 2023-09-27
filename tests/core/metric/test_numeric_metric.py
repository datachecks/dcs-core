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
from datachecks.core.datasource.search_datasource import SearchIndexDataSource
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.numeric_metric import (
    AvgMetric,
    DistinctCountMetric,
    DuplicateCountMetric,
    EmptyStringCountMetric,
    EmptyStringPercentageMetric,
    MaxMetric,
    MinMetric,
    NullCountMetric,
    NullPercentageMetric,
    SumMetric,
    VarianceMetric,
)


class TestMinColumnValueMetric:
    def test_should_return_min_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"where_clause": "age >= 100 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13

    def test_should_return_min_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_min.return_value = 13

        row = MinMetric(
            name="min_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MIN,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 100, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 13
        assert row_value.field_name == "age"


class TestMaxColumnValueMetric:
    def test_should_return_max_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51

    def test_should_return_max_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_max.return_value = 51

        row = MaxMetric(
            name="max_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.MAX,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 51


class TestAvgColumnValueMetric:
    def test_should_return_avg_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_avg_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_avg.return_value = 1.3

        row = AvgMetric(
            name="avg_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.AVG,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3


class TestSumColumnValueMetric:
    def test_should_return_sum_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_sum.return_value = 1.3

        row = SumMetric(
            name="sum_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.SUM,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_sum_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_sum.return_value = 1.3

        row = SumMetric(
            name="sum_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.SUM,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_sum_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_sum.return_value = 1.3

        row = SumMetric(
            name="sum_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.SUM,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3

    def test_should_return_sum_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_sum.return_value = 1.3

        row = SumMetric(
            name="sum_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.SUM,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 1.3


class TestVarianceColumnValueMetric:
    def test_should_return_variance_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_variance.return_value = 4380976080

        row = VarianceMetric(
            name="variance_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.VARIANCE,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 4380976080

    def test_should_return_variance_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_variance.return_value = 4380976080

        row = VarianceMetric(
            name="variance_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.VARIANCE,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 4380976080

    def test_should_return_variance_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_variance.return_value = 4380976080

        row = VarianceMetric(
            name="variance_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.VARIANCE,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 4380976080

    def test_should_return_variance_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_variance.return_value = 4380976080

        row = VarianceMetric(
            name="variance_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.VARIANCE,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 4380976080


class TestDuplicateCountColumnValueMetric:
    def test_should_return_duplicate_count_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_duplicate_count.return_value = 0

        row = DuplicateCountMetric(
            name="duplicate_count_metric_test",
            data_source=mock_data_source,
            table_name="uniqueness_metric_test",
            metric_type=MetricsType.DUPLICATE_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_duplicate_count_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_duplicate_count.return_value = 0

        row = DuplicateCountMetric(
            name="duplicate_count_metric_test_1",
            data_source=mock_data_source,
            table_name="uniqueness_metric_test",
            metric_type=MetricsType.DUPLICATE_COUNT,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_duplicate_count_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_duplicate_count.return_value = 0

        row = DuplicateCountMetric(
            name="duplicate_count_metric_test",
            data_source=mock_data_source,
            index_name="uniqueness_metric_test",
            metric_type=MetricsType.DUPLICATE_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_duplicate_count_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_duplicate_count.return_value = 0

        row = DuplicateCountMetric(
            name="duplicate_count_metric_test_1",
            data_source=mock_data_source,
            index_name="uniqueness_metric_test",
            metric_type=MetricsType.DUPLICATE_COUNT,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0


class TestNullCountColumnValueMetric:
    def test_should_return_null_count_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_count.return_value = 0

        row = NullCountMetric(
            name="null_count_metric_test",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.NULL_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_count_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_count.return_value = 0

        row = NullCountMetric(
            name="null_count_metric_test_1",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.NULL_COUNT,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_count_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_count.return_value = 0

        row = NullCountMetric(
            name="null_count_metric_test",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.NULL_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_count_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_count.return_value = 0

        row = NullCountMetric(
            name="null_count_metric_test_1",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.NULL_COUNT,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0


class TestNullPercentageColumnValueMetric:
    def test_should_return_null_percentage_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_percentage.return_value = 0

        row = NullPercentageMetric(
            name="null_percentage_metric_test",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.NULL_PERCENTAGE,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_percentage_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_percentage.return_value = 0

        row = NullPercentageMetric(
            name="null_percentage_metric_test_1",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.NULL_PERCENTAGE,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_percentage_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_percentage.return_value = 0

        row = NullPercentageMetric(
            name="null_percentage_metric_test",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.NULL_PERCENTAGE,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_null_percentage_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_null_percentage.return_value = 0

        row = NullPercentageMetric(
            name="null_percentage_metric_test_1",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.NULL_PERCENTAGE,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0


class TestDistinctCountColumnValueMetric:
    def test_should_return_distinct_count_column_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_distinct_count.return_value = 100

        row = DistinctCountMetric(
            name="distinct_count_metric_test",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.DISTINCT_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 100

    def test_should_return_distinct_count_column_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_distinct_count.return_value = 50

        row = DistinctCountMetric(
            name="distinct_count_metric_test_1",
            data_source=mock_data_source,
            table_name="numeric_metric_test",
            metric_type=MetricsType.DISTINCT_COUNT,
            field_name="age",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 50

    def test_should_return_distinct_count_column_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_distinct_count.return_value = 75

        row = DistinctCountMetric(
            name="distinct_count_metric_test",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.DISTINCT_COUNT,
            field_name="age",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 75

    def test_should_return_distinct_count_column_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_distinct_count.return_value = 30

        row = DistinctCountMetric(
            name="distinct_count_metric_test_1",
            data_source=mock_data_source,
            index_name="numeric_metric_test",
            metric_type=MetricsType.DISTINCT_COUNT,
            field_name="age",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 30


class TestEmptyStringCountColumnValueMetric:
    def test_should_return_empty_string_count_value_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_count.return_value = 0

        row = EmptyStringCountMetric(
            name="empty_string_count_metric_test",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_COUNT,
            field_name="description",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_empty_string_count_value_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_count.return_value = 0

        row = EmptyStringCountMetric(
            name="empty_string_count_metric_test_1",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_COUNT,
            field_name="description",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_empty_string_count_value_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_count.return_value = 0

        row = EmptyStringCountMetric(
            name="empty_string_count_metric_test",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_COUNT,
            field_name="description",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0

    def test_should_return_empty_string_count_value_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_count.return_value = 0

        row = EmptyStringCountMetric(
            name="empty_string_count_metric_test_1",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_COUNT,
            field_name="description",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0


class TestEmptyStringPercentageColumnValueMetric:
    def test_should_return_empty_string_percentage_postgres_without_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_percentage.return_value = 0.0

        row = EmptyStringPercentageMetric(
            name="empty_string_percentage_metric_test",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_PERCENTAGE,
            field_name="description",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0.0

    def test_should_return_empty_string_percentage_postgres_with_filter(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_percentage.return_value = 0.0

        row = EmptyStringPercentageMetric(
            name="empty_string_percentage_metric_test_1",
            data_source=mock_data_source,
            table_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_PERCENTAGE,
            field_name="description",
            filters={"where_clause": "age >= 30 AND age <= 200"},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0.0

    def test_should_return_empty_string_percentage_opensearch_without_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_percentage.return_value = 0.0

        row = EmptyStringPercentageMetric(
            name="empty_string_percentage_metric_test",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_PERCENTAGE,
            field_name="description",
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0.0

    def test_should_return_empty_string_percentage_opensearch_with_filter(self):
        mock_data_source = Mock(spec=SearchIndexDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_empty_string_percentage.return_value = 0.0

        row = EmptyStringPercentageMetric(
            name="empty_string_percentage_metric_test_1",
            data_source=mock_data_source,
            index_name="completeness_metric_test",
            metric_type=MetricsType.EMPTY_STRING_PERCENTAGE,
            field_name="description",
            filters={"search_query": '{"range": {"age": {"gte": 30, "lte": 200}}}'},
        )
        row_value = row.get_metric_value()
        assert row_value.value == 0.0
