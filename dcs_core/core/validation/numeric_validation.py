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

import re
from typing import Union

from dcs_core.core.datasource.search_datasource import SearchIndexDataSource
from dcs_core.core.datasource.sql_datasource import SQLDataSource
from dcs_core.core.validation.base import Validation
from dcs_core.integrations.databases.oracle import OracleDataSource


class MinValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_min(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_min(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class MaxValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_max(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_max(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class AvgValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_avg(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_avg(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class SumValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_sum(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_sum(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class VarianceValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_variance(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_variance(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class StdDevValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_stddev(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_stddev(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class Percentile20Validation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_percentile(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                percentile=0.2,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for Percentile20Validation")


class Percentile40Validation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_percentile(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                percentile=0.4,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for Percentile40Validation")


class Percentile60Validation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_percentile(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                percentile=0.6,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for Percentile60Validation")


class Percentile80Validation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_percentile(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                percentile=0.8,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for Percentile80Validation")


class Percentile90Validation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_percentile(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                percentile=0.9,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for Percentile90Validation")


class CountZeroValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> int:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_zero_metric(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                operation="count",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for CountZeroValidation")


class PercentZeroValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_zero_metric(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                operation="percent",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for PercentZeroValidation")


class CountNegativeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> int:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_negative_metric(
                table=self.dataset_name,
                field=self.field_name,
                operation="count",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Unsupported data source type for CountNegativeValidation")


class PercentNegativeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> float:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_negative_metric(
                table=self.dataset_name,
                field=self.field_name,
                operation="percent",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError(
                "Unsupported data source type for PercentNegativeValidation"
            )
