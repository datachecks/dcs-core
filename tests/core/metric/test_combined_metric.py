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

from unittest.mock import Mock

from datachecks.core.common.models.metric import MetricsType, MetricValue
from datachecks.core.common.models.validation import Threshold, Validation
from datachecks.core.metric.combined_metric import CombinedMetric


class TestCombinedMetric:
    def test_should_return_combined_value_without_filter(self, mocker):
        mocker.patch(
            "datachecks.core.metric.combined_metric.CombinedMetric._metric_expression_parser",
            return_value=None,
        )
        mocker.patch(
            "datachecks.core.metric.combined_metric.CombinedMetric._perform_operation",
            return_value=51,
        )
        combined = CombinedMetric(
            name="combined_metric_test",
            metric_type=MetricsType.COMBINED,
            expression="mul(2, 3)",
        )
        combined_value = combined.get_metric_value(metric_values=[])
        assert combined_value.value == 51

    def test_should_perform_operation(self):
        combined = CombinedMetric(
            name="combined_metric_test",
            metric_type=MetricsType.COMBINED,
            expression="mul(2, 3)",
        )
        assert combined._perform_operation({"operation": "mul", "args": [2, 3]}) == 6
        assert combined._perform_operation({"operation": "sum", "args": [2, 3]}) == 5
        assert combined._perform_operation({"operation": "sub", "args": [2, 3]}) == -1
        assert (
            combined._perform_operation({"operation": "div", "args": [2, 3]})
            == 0.6666666666666666
        )
        assert (
            combined._perform_operation(
                {"operation": "sum", "args": [{"operation": "sum", "args": [2, 3]}, 3]}
            )
            == 8
        )
        try:
            assert (
                combined._perform_operation({"operation": "mod", "args": [2, 3]})
                == 2 % 3
            )
        except Exception as e:
            assert str(e) == "Invalid operation"

    def test_should_metric_expression_parser(self):
        metric_value = Mock(MetricValue)
        metric_value.value = 2
        metric_value.tags = {"metric_name": "test_metric"}

        combined = CombinedMetric(
            name="combined_metric_test",
            metric_type=MetricsType.COMBINED,
            expression="mul(test_metric, 3)",
        )

        assert combined._metric_expression_parser("mul(2, 3)", [Mock(MetricValue)]) == {
            "operation": "mul",
            "args": [2, 3],
        }

    def test_should_throw_exception_on_invalid_expression(self):
        combined = CombinedMetric(
            name="combined_metric_test",
            metric_type=MetricsType.COMBINED,
            expression="mul(2, 3",
        )
        metric_value = Mock(MetricValue)
        metric_value.value = 2
        metric_value.tags = {"metric_name": "test_metric"}

        try:
            combined._metric_expression_parser("mul(2, 3", [metric_value])
        except Exception as e:
            assert (
                str(e)
                == "('Invalid expression mul(2, 3', ValueError('Metric mul not found in a mul(2, 3'))"
            )
        try:
            combined._metric_expression_parser("maximum(2, 3)", [metric_value])
        except Exception as e:
            assert str(e) == "Invalid Operation"
        try:
            combined._metric_expression_parser("mul(2, 3, 5)", [metric_value])
        except Exception as e:
            assert str(e) == "Operation must have only two arguments"

    def test_should_return_combined_value_with_validation(self, mocker):
        mocker.patch(
            "datachecks.core.metric.combined_metric.CombinedMetric._metric_expression_parser",
            return_value=None,
        )
        mocker.patch(
            "datachecks.core.metric.combined_metric.CombinedMetric._perform_operation",
            return_value=51,
        )
        combined = CombinedMetric(
            name="combined_metric_test",
            metric_type=MetricsType.COMBINED,
            expression="mul(2, 3)",
            validation=Validation(threshold=Threshold(gt=100)),
        )
        combined_value = combined.get_metric_value(metric_values=[])
        assert combined_value.value == 51
        assert combined_value.is_valid == False
