
import unittest.mock as mock
import pytest
from datachecks.core.logger.default_logger import DefaultLogger


class TestDefaultLogger:

    def test_default_logger_should_call_log_method(self):
        logger = DefaultLogger()
        logging_input = ("asd", 1.0, {"dataSourceName": "asd", "metricType": "asd", "identity": "asd"})
        logger.log(*logging_input)

        with mock.patch.object(logger, 'log',
                               wraps=logger.log) as monkey:
            logger.log("asd", 1.0, {"dataSourceName": "asd", "metricType": "asd", "identity": "asd"})
            monkey.assert_called_with(*logging_input)
