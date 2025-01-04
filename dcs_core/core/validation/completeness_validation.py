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


class CountNullValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_null_count(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_null_count(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class PercentageNullValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_null_percentage(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_null_percentage(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class CountEmptyStringValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_empty_string_count(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_empty_string_count(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class PercentageEmptyStringValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            if isinstance(self.data_source, OracleDataSource) and self.where_filter:
                self.where_filter = re.sub(
                    r"(\b[a-zA-Z_]+\b)(?=\s*[=<>])", r'"\1"', self.where_filter
                )
            return self.data_source.query_get_empty_string_percentage(
                table=self.dataset_name,
                field=f'"{self.field_name}"'
                if isinstance(self.data_source, OracleDataSource)
                else self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
        elif isinstance(self.data_source, SearchIndexDataSource):
            return self.data_source.query_get_empty_string_percentage(
                index_name=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter else None,
            )
        else:
            raise ValueError("Invalid data source type")


class CountAllSpaceValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_all_space_count(
                table=self.dataset_name,
                field=self.field_name,
                operation="count",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Invalid data source type")


class PercentageAllSpaceValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_all_space_count(
                table=self.dataset_name,
                field=self.field_name,
                operation="percent",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Invalid data source type")


class CountNullKeywordValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_null_keyword_count(
                table=self.dataset_name,
                field=self.field_name,
                operation="count",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Invalid data source type")


class PercentageNullKeywordValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_null_keyword_count(
                table=self.dataset_name,
                field=self.field_name,
                operation="percent",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError("Invalid data source type")
