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

from typing import Union

from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.validation.base import Validation


class CountUUIDValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="uuid",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "UUID validation is only supported for SQL data sources"
            )


class PercentUUIDValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="uuid",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "UUID validation is only supported for SQL data sources"
            )
