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

from typing import Dict, Optional

from datachecks.core.common.models.configuration import (
    Configuration,
    ValidationConfigByDataset,
)
from datachecks.core.common.models.validation import ValidationFunction
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.validation.base import Validation
from datachecks.core.validation.completeness_validation import (  # noqa F401 this is used in globals
    CountEmptyStringValidation,
    CountNullValidation,
    PercentageEmptyStringValidation,
    PercentageNullValidation,
)
from datachecks.core.validation.custom_query_validation import (  # noqa F401 this is used in globals
    CustomSqlValidation,
)
from datachecks.core.validation.numeric_validation import (  # noqa F401 this is used in globals
    AvgValidation,
    MaxValidation,
    MinValidation,
    StdDevValidation,
    SumValidation,
    VarianceValidation,
)
from datachecks.core.validation.reliability_validation import (  # noqa F401 this is used in globals
    CountDocumentsValidation,
    CountRowValidation,
    FreshnessValueMetric,
)
from datachecks.core.validation.uniqueness_validation import (  # noqa F401 this is used in globals
    CountDistinctValidation,
    CountDuplicateValidation,
)
from datachecks.core.validation.validity_validation import (  # noqa F401 this is used in globals
    CountUUIDValidation,
    PercentUUIDValidation,
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
        ValidationFunction.FRESHNESS.value: "FreshnessValueMetric",
        ValidationFunction.COUNT_UUID.value: "CountUUIDValidation",
        ValidationFunction.PERCENT_UUID.value: "PercentUUIDValidation",
        ValidationFunction.STRING_LENGTH_MAX.value: "StringLengthMaxValidation",
        ValidationFunction.STRING_LENGTH_MIN.value: "StringLengthMinValidation",
        ValidationFunction.STRING_LENGTH_AVERAGE.value: "StringLengthAverageValidation",
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
