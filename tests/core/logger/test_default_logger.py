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

import unittest.mock as mock

import pytest

from dcs_core.core.logger.default_logger import DefaultLogger


class TestDefaultLogger:
    def test_default_logger_should_call_log_method(self):
        logger = DefaultLogger()
        logging_input = (
            "asd",
            1.0,
            {"dataSourceName": "asd", "metricType": "asd", "identity": "asd"},
        )
        logger.log(*logging_input)

        with mock.patch.object(logger, "log", wraps=logger.log) as monkey:
            logger.log(
                "asd",
                1.0,
                {"dataSourceName": "asd", "metricType": "asd", "identity": "asd"},
            )
            monkey.assert_called_with(*logging_input)
