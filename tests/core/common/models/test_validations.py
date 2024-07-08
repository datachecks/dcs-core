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

import pytest

from datachecks.core.common.models.configuration import ValidationConfig


class TestValidationV1:
    def test_on_field_is_required(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on=None)

    def test_should_have_valid_column_validation_function(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on="minn(age)")

    def test_should_have_valid_dataset_validation_function(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on="row_count")

    def test_table_validation_function_should_not_have_brackets(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on="count_rows(age)", threshold="> 0")

    def test_validation_function_malformed_with_space_should_not_throw_error(self):
        with pytest.raises(ValueError):
            validation = ValidationConfig(
                name="test", on="count_rows ", threshold="> 0"
            )

    def test_on_filed_with_valid_function_should_not_throw_error(self):
        validation = ValidationConfig(name="test", on="min(age)", threshold="> 0")
        assert validation.on == "min(age)"

    def test_on_field_with_invalid_function_should_throw_error(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on="min(age", threshold="> 0")

    def test_column_name_should_not_have_special_char(self):
        with pytest.raises(ValueError):
            ValidationConfig(name="test", on="min(age@)", threshold="> 0")
