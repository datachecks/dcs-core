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

import pathlib

from datachecks.core.configuration.configuration_parser_v1 import (
    Configuration,
    load_configuration,
)

current_path = pathlib.Path(__file__).parent.resolve()


def test_should_parse_single_config_file():
    configuration: Configuration = load_configuration(
        f"{current_path}/test_config_v1.yaml"
    )
    assert configuration is not None
    assert len(configuration.data_sources) == 3
    assert len(configuration.validations.keys()) == 3


def test_should_parse_multiple_config_files():
    configuration: Configuration = load_configuration(
        f"{current_path}/test_configurations_v1/"
    )
    assert configuration is not None
    assert len(configuration.data_sources) == 2
    assert len(configuration.validations.keys()) == 2

    assert len(configuration.validations["search_staging_db.products"].validations) == 1
    assert (
        len(configuration.validations["search_datastore.product_data_us"].validations)
        == 1
    )
