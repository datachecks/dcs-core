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

from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.profiling.numeric_field_profiling import NumericSQLFieldProfiler


class TestNumericSQLFieldProfiler:
    def test_should_generate_numeric_sql_field_profiler(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_table_metadata.return_value = ["test_table"]
        mock_data_source.query_get_column_metadata.return_value = {"test_field": "int"}
        mock_data_source.profiling_sql_aggregates_numeric.return_value = {
            "min": 1,
            "max": 2,
            "avg": 1.5,
            "sum": 3,
            "stddev": 0.5,
            "variance": 0.25,
            "distinct_count": 2,
            "missing_count": 0,
        }

        profiler = NumericSQLFieldProfiler(
            data_source=mock_data_source,
            table_name="test_table",
            field_name="test_field",
            data_type="int",
        )
        field_profile = profiler.generate()

        assert field_profile.field_name == "test_field"
        assert field_profile.data_type == "int"
        assert field_profile.min.value == 1
        assert field_profile.max.value == 2
        assert field_profile.avg.value == 1.5
        assert field_profile.sum.value == 3
        assert field_profile.stddev.value == 0.5
        assert field_profile.variance.value == 0.25
        assert field_profile.distinct_count.value == 2
        assert field_profile.missing_count.value == 0

    def test_should_handle_null_values(self):
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name = "test_data_source"
        mock_data_source.query_get_table_metadata.return_value = ["test_table"]
        mock_data_source.query_get_column_metadata.return_value = {"test_field": "int"}
        mock_data_source.profiling_sql_aggregates_numeric.return_value = {
            "min": None,
            "max": None,
            "avg": None,
            "sum": None,
            "stddev": None,
            "variance": None,
            "distinct_count": None,
            "missing_count": None,
        }

        profiler = NumericSQLFieldProfiler(
            data_source=mock_data_source,
            table_name="test_table",
            field_name="test_field",
            data_type="int",
        )
        field_profile = profiler.generate()

        assert field_profile.field_name == "test_field"
        assert field_profile.data_type == "int"
        assert field_profile.min.value is None
        assert field_profile.max.value is None
        assert field_profile.avg.value is None
        assert field_profile.sum.value is None
        assert field_profile.stddev.value is None
        assert field_profile.variance.value is None
        assert field_profile.distinct_count.value is None
        assert field_profile.missing_count.value is None
