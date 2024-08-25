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

from datachecks.core.datasource.search_datasource import SearchIndexDataSource
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.validation.base import Validation


class CountUUIDValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                regex_pattern=self.regex_pattern,
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
                regex_pattern=self.regex_pattern,
                predefined_regex_pattern="uuid",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "UUID validation is only supported for SQL data sources"
            )


class CountInvalidValues(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.values is None:
            raise ValueError("Values are required for count_invalid_values validation")
        if isinstance(self.data_source, SQLDataSource):
            (
                invalid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                values=self.values,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return invalid_count
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class PercentInvalidValues(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.values is None:
            raise ValueError(
                "Values are required for percent_invalid_values validation"
            )
        if isinstance(self.data_source, SQLDataSource):
            (
                invalid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                values=self.values,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(invalid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class CountValidValues(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.values is None:
            raise ValueError("Values are required for count_valid_values validation")
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                values=self.values,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class PercentValidValues(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.values is None:
            raise ValueError("Values are required for percent_valid_values validation")
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                values=self.values,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class CountInvalidRegex(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.regex_pattern is None:
            raise ValueError(
                "Regex pattern is required for count_invalid_regex validation"
            )
        if isinstance(self.data_source, SQLDataSource):
            (
                invalid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                regex_pattern=self.regex_pattern,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return invalid_count
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class PercentInvalidRegex(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.regex_pattern is None:
            raise ValueError(
                "Regex pattern is required for percent_invalid_regex validation"
            )
        if isinstance(self.data_source, SQLDataSource):
            (
                invalid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                regex_pattern=self.regex_pattern,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(invalid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class CountValidRegex(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.regex_pattern is None:
            raise ValueError(
                "Regex pattern is required for count_valid_regex validation"
            )
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                regex_pattern=self.regex_pattern,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class PercentValidRegex(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if self.regex_pattern is None:
            raise ValueError(
                "Regex pattern is required for percent_valid_regex validation"
            )
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_valid_invalid_values_validity(
                table=self.dataset_name,
                field=self.field_name,
                regex_pattern=self.regex_pattern,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "Valid/Invalid values validation is only supported for SQL data sources"
            )


class CountUSAPhoneValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_phone",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        elif isinstance(self.data_source, SearchIndexDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                index_name=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_phone",
                filters=self.where_filter if self.where_filter else None,
            )
            return valid_count
        else:
            raise ValueError("Invalid data source type")


class PercentUSAPhoneValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_phone",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        elif isinstance(self.data_source, SearchIndexDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                index_name=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_phone",
                filters=self.where_filter if self.where_filter else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise ValueError("Invalid data source type")


class CountEmailValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="email",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "Email validation is only supported for SQL data sources"
            )


class PercentEmailValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="email",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "Email validation is only supported for SQL data sources"
            )


class StringLengthMaxValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_string_length_metric(
                table=self.dataset_name,
                field=self.field_name,
                metric="max",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError(
                "Unsupported data source type for StringLengthMaxValidation"
            )


class StringLengthMinValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_string_length_metric(
                table=self.dataset_name,
                field=self.field_name,
                metric="min",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError(
                "Unsupported data source type for StringLengthMinValidation"
            )


class StringLengthAverageValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_string_length_metric(
                table=self.dataset_name,
                field=self.field_name,
                metric="avg",
                filters=self.where_filter if self.where_filter is not None else None,
            )
        else:
            raise ValueError(
                "Unsupported data source type for StringLengthAverageValidation"
            )


class CountUSAZipCodeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_zip_code",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "USA Zip Code validation is only supported for SQL data sources"
            )


class PercentUSAZipCodeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            valid_count, total_count = self.data_source.query_string_pattern_validity(
                table=self.dataset_name,
                field=self.field_name,
                predefined_regex_pattern="usa_zip_code",
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "USA Zip Code validation is only supported for SQL data sources"
            )


class CountUSAStateCodeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_get_usa_state_code_validity(
                table=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return valid_count
        else:
            raise NotImplementedError(
                "USA State Code validation is only supported for SQL data sources"
            )


class PercentUSAStateCodeValidation(Validation):
    def _generate_metric_value(self, **kwargs) -> Union[float, int]:
        if isinstance(self.data_source, SQLDataSource):
            (
                valid_count,
                total_count,
            ) = self.data_source.query_get_usa_state_code_validity(
                table=self.dataset_name,
                field=self.field_name,
                filters=self.where_filter if self.where_filter is not None else None,
            )
            return round(valid_count / total_count * 100, 2) if total_count > 0 else 0
        else:
            raise NotImplementedError(
                "USA State Code validation is only supported for SQL data sources"
            )
