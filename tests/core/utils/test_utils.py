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

import os

import pytest

from datachecks.core.utils.utils import ensure_directory_exists

TEST_DIR = "/tmp/datachecks/test_dir"


@pytest.fixture(scope="module", autouse=True)
def directory_fixture():
    try:
        os.rmdir(TEST_DIR)
    except FileNotFoundError:
        pass
    yield
    try:
        os.rmdir(TEST_DIR)
    except FileNotFoundError:
        pass


def test_check_directory_exists():
    os.makedirs(TEST_DIR)
    assert ensure_directory_exists(TEST_DIR) is True


def test_check_and_create_dir():
    status = ensure_directory_exists(TEST_DIR, create_if_not_exists=True)
    assert status is True
