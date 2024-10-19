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

from typing import Dict

from dcs_core.core.common.models.configuration import (
    Configuration,
    ValidationConfigByDataset,
)
from dcs_core.core.common.models.validation import ValidationFunction
from dcs_core.core.datasource.manager import DataSourceManager
from dcs_core.core.validation.base import DeltaValidation, Validation
from dcs_core.core.validation.completeness_validation import (  # noqa F401 this is used in globals
    CountAllSpaceValidation,
    CountEmptyStringValidation,
    CountNullKeywordValidation,
    CountNullValidation,
    PercentageAllSpaceValidation,
    PercentageEmptyStringValidation,
    PercentageNullKeywordValidation,
    PercentageNullValidation,
)
from dcs_core.core.validation.custom_query_validation import (  # noqa F401 this is used in globals
    CustomSqlValidation,
)
from dcs_core.core.validation.numeric_validation import (  # noqa F401 this is used in globals
    AvgValidation,
    CountNegativeValidation,
    CountZeroValidation,
    MaxValidation,
    MinValidation,
    Percentile20Validation,
    Percentile40Validation,
    Percentile60Validation,
    Percentile80Validation,
    Percentile90Validation,
    PercentNegativeValidation,
    PercentZeroValidation,
    StdDevValidation,
    SumValidation,
    VarianceValidation,
)
from dcs_core.core.validation.reliability_validation import (  # noqa F401 this is used in globals
    CountDocumentsValidation,
    CountRowValidation,
    DeltaCountRowValidation,
    FreshnessValueMetric,
)
from dcs_core.core.validation.uniqueness_validation import (  # noqa F401 this is used in globals
    CountDistinctValidation,
    CountDuplicateValidation,
)
from dcs_core.core.validation.validity_validation import (  # noqa F401 this is used in globals
    CountCUSIPValidation,
    CountDateNotInFutureValidation,
    CountEmailValidation,
    CountFIGIValidation,
    CountInvalidRegex,
    CountInvalidValues,
    CountISINValidation,
    CountLatitudeValidation,
    CountLEIValidation,
    CountLongitudeValidation,
    CountNotInFutureValidation,
    CountPermIDValidation,
    CountSEDOLValidation,
    CountSSNValidation,
    CountTimeStampValidation,
    CountUSAPhoneValidation,
    CountUSAStateCodeValidation,
    CountUSAZipCodeValidation,
    CountUUIDValidation,
    CountValidRegex,
    CountValidValues,
    PercentCUSIPValidation,
    PercentDateNotInFutureValidation,
    PercentEmailValidation,
    PercentFIGIValidation,
    PercentInvalidRegex,
    PercentInvalidValues,
    PercentISINValidation,
    PercentLatitudeValidation,
    PercentLEIValidation,
    PercentLongitudeValidation,
    PercentNotInFutureValidation,
    PercentPermIDValidation,
    PercentSEDOLValidation,
    PercentSSNValidation,
    PercentTimeStampValidation,
    PercentUSAPhoneValidation,
    PercentUSAStateCodeValidation,
    PercentUSAZipCodeValidation,
    PercentUUIDValidation,
    PercentValidRegex,
    PercentValidValues,
    StringLengthAverageValidation,
    StringLengthMaxValidation,
    StringLengthMinValidation,
)


class ValidationManager:
    VALIDATION_CLASS_MAPPING = {
        ValidationFunction.MIN.value: "MinValidation",
        ValidationFunction.MAX.value: "MaxValidation",
        ValidationFunction.AVG.value: "AvgValidation",
        ValidationFunction.SUM.value: "SumValidation",
        ValidationFunction.VARIANCE.value: "VarianceValidation",
        ValidationFunction.STDDEV.value: "StdDevValidation",
        ValidationFunction.COUNT_DUPLICATE.value: "CountDuplicateValidation",
        ValidationFunction.COUNT_DISTINCT.value: "CountDistinctValidation",
        ValidationFunction.COUNT_NULL.value: "CountNullValidation",
        ValidationFunction.PERCENT_NULL.value: "PercentageNullValidation",
        ValidationFunction.COUNT_EMPTY_STRING.value: "CountEmptyStringValidation",
        ValidationFunction.PERCENT_EMPTY_STRING.value: "PercentageEmptyStringValidation",
        ValidationFunction.CUSTOM_SQL.value: "CustomSqlValidation",
        ValidationFunction.COUNT_DOCUMENTS.value: "CountDocumentsValidation",
        ValidationFunction.COUNT_ROWS.value: "CountRowValidation",
        ValidationFunction.DELTA_COUNT_ROWS.value: "DeltaCountRowValidation",
        ValidationFunction.FRESHNESS.value: "FreshnessValueMetric",
        ValidationFunction.COUNT_UUID.value: "CountUUIDValidation",
        ValidationFunction.PERCENT_UUID.value: "PercentUUIDValidation",
        ValidationFunction.COUNT_INVALID_VALUES.value: "CountInvalidValues",
        ValidationFunction.PERCENT_INVALID_VALUES.value: "PercentInvalidValues",
        ValidationFunction.COUNT_VALID_VALUES.value: "CountValidValues",
        ValidationFunction.PERCENT_VALID_VALUES.value: "PercentValidValues",
        ValidationFunction.COUNT_INVALID_REGEX.value: "CountInvalidRegex",
        ValidationFunction.PERCENT_INVALID_REGEX.value: "PercentInvalidRegex",
        ValidationFunction.COUNT_VALID_REGEX.value: "CountValidRegex",
        ValidationFunction.PERCENT_VALID_REGEX.value: "PercentValidRegex",
        ValidationFunction.COUNT_USA_PHONE.value: "CountUSAPhoneValidation",
        ValidationFunction.PERCENT_USA_PHONE.value: "PercentUSAPhoneValidation",
        ValidationFunction.COUNT_EMAIL.value: "CountEmailValidation",
        ValidationFunction.PERCENT_EMAIL.value: "PercentEmailValidation",
        ValidationFunction.STRING_LENGTH_MAX.value: "StringLengthMaxValidation",
        ValidationFunction.STRING_LENGTH_MIN.value: "StringLengthMinValidation",
        ValidationFunction.STRING_LENGTH_AVERAGE.value: "StringLengthAverageValidation",
        ValidationFunction.COUNT_USA_STATE_CODE.value: "CountUSAStateCodeValidation",
        ValidationFunction.PERCENT_USA_STATE_CODE.value: "PercentUSAStateCodeValidation",
        ValidationFunction.COUNT_USA_ZIP_CODE.value: "CountUSAZipCodeValidation",
        ValidationFunction.PERCENT_USA_ZIP_CODE.value: "PercentUSAZipCodeValidation",
        ValidationFunction.COUNT_LATITUDE.value: "CountLatitudeValidation",
        ValidationFunction.PERCENT_LATITUDE.value: "PercentLatitudeValidation",
        ValidationFunction.COUNT_LONGITUDE.value: "CountLongitudeValidation",
        ValidationFunction.PERCENT_LONGITUDE.value: "PercentLongitudeValidation",
        ValidationFunction.COUNT_SSN.value: "CountSSNValidation",
        ValidationFunction.PERCENT_SSN.value: "PercentSSNValidation",
        ValidationFunction.COUNT_SEDOL.value: "CountSEDOLValidation",
        ValidationFunction.PERCENT_SEDOL.value: "PercentSEDOLValidation",
        ValidationFunction.COUNT_CUSIP.value: "CountCUSIPValidation",
        ValidationFunction.PERCENT_CUSIP.value: "PercentCUSIPValidation",
        ValidationFunction.COUNT_LEI.value: "CountLEIValidation",
        ValidationFunction.PERCENT_LEI.value: "PercentLEIValidation",
        ValidationFunction.COUNT_FIGI.value: "CountFIGIValidation",
        ValidationFunction.PERCENT_FIGI.value: "PercentFIGIValidation",
        ValidationFunction.COUNT_ISIN.value: "CountISINValidation",
        ValidationFunction.PERCENT_ISIN.value: "PercentISINValidation",
        ValidationFunction.COUNT_PERM_ID.value: "CountPermIDValidation",
        ValidationFunction.PERCENT_PERM_ID.value: "PercentPermIDValidation",
        ValidationFunction.PERCENTILE_20.value: "Percentile20Validation",
        ValidationFunction.PERCENTILE_40.value: "Percentile40Validation",
        ValidationFunction.PERCENTILE_60.value: "Percentile60Validation",
        ValidationFunction.PERCENTILE_80.value: "Percentile80Validation",
        ValidationFunction.PERCENTILE_90.value: "Percentile90Validation",
        ValidationFunction.COUNT_ZERO.value: "CountZeroValidation",
        ValidationFunction.PERCENT_ZERO.value: "PercentZeroValidation",
        ValidationFunction.COUNT_NEGATIVE.value: "CountNegativeValidation",
        ValidationFunction.PERCENT_NEGATIVE.value: "PercentNegativeValidation",
        ValidationFunction.COUNT_ALL_SPACE.value: "CountAllSpaceValidation",
        ValidationFunction.PERCENT_ALL_SPACE.value: "PercentageAllSpaceValidation",
        ValidationFunction.COUNT_NULL_KEYWORD.value: "CountNullKeywordValidation",
        ValidationFunction.PERCENT_NULL_KEYWORD.value: "PercentageNullKeywordValidation",
        ValidationFunction.COUNT_TIMESTAMP_STRING.value: "CountTimeStampValidation",
        ValidationFunction.PERCENT_TIMESTAMP_STRING.value: "PercentTimeStampValidation",
        ValidationFunction.COUNT_NOT_IN_FUTURE.value: "CountNotInFutureValidation",
        ValidationFunction.PERCENT_NOT_IN_FUTURE.value: "PercentNotInFutureValidation",
        ValidationFunction.COUNT_DATE_NOT_IN_FUTURE.value: "CountDateNotInFutureValidation",
        ValidationFunction.PERCENT_DATE_NOT_IN_FUTURE.value: "PercentDateNotInFutureValidation",
    }

    def __init__(
        self,
        application_configs: Configuration,
        data_source_manager: DataSourceManager,
    ):
        self.data_source_manager = data_source_manager
        self.application_configs = application_configs
        self.validation_configs: Dict[
            str, ValidationConfigByDataset
        ] = application_configs.validations

        """
        Will store the validations in the following format:
        {
            "data_source_name": {
                "dataset_name": {
                    "validation_name": Validation
                }
            }
        }
        """
        self.validations: Dict[str, Dict[str, Dict[str, Validation]]] = {}

    def set_validation_configs(self, validations: Dict[str, ValidationConfigByDataset]):
        self.validation_configs = validations

    def build_validations(self):
        for _, validation_by_dataset in self.validation_configs.items():
            data_source_name = validation_by_dataset.data_source
            dataset_name = validation_by_dataset.dataset

            if data_source_name not in self.validations:
                self.validations[data_source_name] = {}

            if dataset_name not in self.validations[data_source_name]:
                self.validations[data_source_name][dataset_name] = {}

            for (
                validation_name,
                validation_config,
            ) in validation_by_dataset.validations.items():
                data_source = self.data_source_manager.get_data_source(data_source_name)
                params = {}
                if validation_config.get_is_delta_validation:
                    reference_data_source = self.data_source_manager.get_data_source(
                        validation_config.get_ref_data_source_name
                    )
                    base_class_name = self.VALIDATION_CLASS_MAPPING[
                        validation_config.get_validation_function
                    ]
                    validation: DeltaValidation = globals()[base_class_name](
                        name=validation_name,
                        data_source=data_source,
                        dataset_name=dataset_name,
                        validation_name=validation_name,
                        validation_config=validation_config,
                        field_name=validation_config.get_validation_field_name,
                        reference_data_source=reference_data_source,
                        reference_dataset_name=validation_config.get_ref_dataset_name,
                        reference_field_name=validation_config.get_ref_field_name,
                        **params,
                    )
                    self.validations[data_source_name][dataset_name][
                        validation_name
                    ] = validation
                else:
                    validation: Validation = globals()[
                        self.VALIDATION_CLASS_MAPPING[
                            validation_config.get_validation_function
                        ]
                    ](
                        name=validation_name,
                        data_source=data_source,
                        dataset_name=dataset_name,
                        validation_name=validation_name,
                        validation_config=validation_config,
                        field_name=validation_config.get_validation_field_name,
                        **params,
                    )
                    self.validations[data_source_name][dataset_name][
                        validation_name
                    ] = validation

    def add_validation(self, validation: Validation):
        data_source_name = validation.data_source.data_source_name
        dataset_name = validation.dataset_name
        validation_name = validation.name
        if data_source_name not in self.validations:
            self.validations[data_source_name] = {}

        if dataset_name not in self.validations[data_source_name]:
            self.validations[data_source_name][dataset_name] = {}

        self.validations[data_source_name][dataset_name][validation_name] = validation

    @property
    def get_validations(self):
        return self.validations

    def get_validation(
        self, data_source_name: str, dataset_name: str, validation_name: str
    ) -> Validation:
        return self.validations[data_source_name][dataset_name][validation_name]
