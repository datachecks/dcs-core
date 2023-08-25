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

from datachecks.core.common.models.metric import MetricsType, MetricValue
from datachecks.core.common.models.profile import NumericFieldProfile, TextFieldProfile
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.profiling.datasource_profiling import DataSourceProfiling


class TestFieldProfiling:
    def test_should_generate_profile_for_sql_datasource_numeric_field(self, mocker):
        mocker.patch(
            "datachecks.core.profiling.numeric_field_profiling.NumericSQLFieldProfiler.generate",
            return_value=NumericFieldProfile(
                field_name="test_field",
                data_type="int",
                min=MetricValue(
                    value=1,
                    identity="test_identity",
                    metric_type=MetricsType("min"),
                    timestamp="2020-01-01T00:00:00.000000",
                    data_source="test_data_source",
                ),
            ),
        )
        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name.return_value = "test_data_source"
        mock_data_source.query_get_table_metadata.return_value = ["test_table"]
        mock_data_source.query_get_column_metadata.return_value = {"test_field": "int"}

        field_profile = DataSourceProfiling(mock_data_source)
        list_metric = field_profile.generate()

        assert len(list_metric) == 1
        assert list_metric[0].table_name == "test_table"

    def test_should_generate_profile_for_sql_datasource_string_field(self, mocker):
        mocker.patch(
            "datachecks.core.profiling.text_field_profiling.TextSQLFieldProfiler.generate",
            return_value=TextFieldProfile(
                field_name="test_field",
                data_type="str",
                distinct_count=MetricValue(
                    value=1,
                    identity="test_identity",
                    metric_type=MetricsType("min"),
                    timestamp="2020-01-01T00:00:00.000000",
                    data_source="test_data_source",
                    table_name="test_table",
                ),
            ),
        )

        mock_data_source = Mock(spec=SQLDataSource)
        mock_data_source.data_source_name.return_value = "test_data_source"
        mock_data_source.query_get_table_metadata.return_value = ["test_table"]
        mock_data_source.query_get_column_metadata.return_value = {"test_field": "str"}

        field_profile = DataSourceProfiling(mock_data_source)
        list_metric = field_profile.generate()

        assert len(list_metric) == 1
