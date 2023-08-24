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

from tests.cli.run_cli import run_cli


def test_should_exit_if_config_path_not_provided():
    run_result = run_cli(
        [
            "inspect",
        ]
    )
    assert run_result.exit_code == 2
    assert "Error: Missing option '-C' / '--config-path'" in run_result.output


def test_should_exit_if_config_path_does_not_exist():
    run_result = run_cli(
        [
            "inspect",
            "-C",
            "non-existing-file.yml",
        ]
    )
    assert run_result.exit_code == 1
