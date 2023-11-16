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
from datetime import datetime, timezone
from typing import Dict, List, Union

from datachecks.core.common.models.metric import (
    IndexMetrics,
    MetricsType,
    MetricValue,
    TableMetrics,
)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.base import MetricIdentity
from datachecks.core.profiling.numeric_field_profiling import NumericSQLFieldProfiler
from datachecks.core.profiling.text_field_profiling import TextSQLFieldProfiler


class DataSourceProfiling:
    """
    This class is responsible for generating field profiles for a given data source.

    """

    def __init__(self, data_source: DataSource):
        """
        :param data_source: The data source for which field profiles are to be generated.
        """
        self._datasource = data_source
        if isinstance(data_source, SQLDataSource):
            self._tables: List[str] = data_source.query_get_table_metadata()
            self._field_meta_data: Dict[str, Dict[str, str]] = {}
            for table in self._tables:
                self._field_meta_data[table] = data_source.query_get_column_metadata(
                    table_name=table
                )

    def _generate_sql_data_source_profiles(self) -> List[TableMetrics]:
        """
        This method generates field profiles for a SQL data source.
        """
        list_of_metric = []
        for table, fields in self._field_meta_data.items():
            table_metrics: List[MetricValue] = []

            for field, data_type in fields.items():
                # profile for numeric fields if the data type is numeric
                if data_type in DataSource.NUMERIC_PYTHON_TYPES_FOR_PROFILING:
                    metrics = self._generate_numeric_field_profile(
                        table=table, field=field, data_type=data_type
                    )
                    table_metrics.extend(metrics)

                # profile for numeric fields if the data type is text
                elif data_type in DataSource.TEXT_PYTHON_TYPES_FOR_PROFILING:
                    metrics = self._generate_text_field_profile(
                        table=table, field=field, data_type=data_type
                    )
                    table_metrics.extend(metrics)

            # add row count metrics
            table_metrics.append(self._generate_sql_table_row_count(table=table))
            # create a table metric list for a table
            list_of_metric.append(
                TableMetrics(
                    table_name=table,
                    metrics={metric.identity: metric for metric in table_metrics},
                    data_source=self._datasource.data_source_name,
                )
            )

        return list_of_metric

    def _generate_sql_table_row_count(self, table: str) -> MetricValue:
        if isinstance(self._datasource, SQLDataSource):
            table_row_count = self._datasource.query_get_row_count(table=table)
            return MetricValue(
                identity=MetricIdentity.generate_identity(
                    metric_name="",
                    metric_type=MetricsType.ROW_COUNT,
                    data_source=self._datasource,
                    table_name=table,
                ),
                value=table_row_count,
                data_source=self._datasource.data_source_name,
                metric_type=MetricsType.ROW_COUNT,
                table_name=table,
                timestamp=datetime.now(timezone.utc),
            )

    def _generate_numeric_field_profile(
        self, table: str, field: str, data_type: str
    ) -> List[MetricValue]:
        """
        This method generates a numeric field profile for a given field.
        """
        profiles = []
        if isinstance(self._datasource, SQLDataSource):
            profiler = NumericSQLFieldProfiler(
                data_source=self._datasource,
                table_name=table,
                field_name=field,
                data_type=data_type,
            )
            generate = profiler.generate()
            profiles = generate.get_metric_values
        return profiles

    def _generate_text_field_profile(
        self, table: str, field: str, data_type: str
    ) -> List[MetricValue]:
        """
        This method generates a text field profile for a given field.
        """
        profiles = []

        if isinstance(self._datasource, SQLDataSource):
            profiler = TextSQLFieldProfiler(
                data_source=self._datasource,
                table_name=table,
                field_name=field,
                data_type=data_type,
            )
            generate = profiler.generate()
            profiles = generate.get_metric_values
        return profiles

    def generate(self) -> List[Union[TableMetrics, IndexMetrics]]:
        """
        This method generates field profiles for a given data source.
        """
        if isinstance(self._datasource, SQLDataSource):
            return self._generate_sql_data_source_profiles()
        else:
            raise NotImplementedError(
                f"Profiling for {self._datasource.data_source_name} is not implemented."
            )
