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

from datetime import datetime, timezone

from dcs_core.core.common.models.metric import MetricsType, MetricValue


class TestMetricValue:
    def test_datetime_serializer(self):
        current_time = datetime.now(timezone.utc)
        metric_value = MetricValue(
            identity="identity",
            value=1,
            metric_type=MetricsType.ROW_COUNT,
            timestamp=current_time,
        )
        json_encoded = metric_value.json
        json_decoded = MetricValue.from_json(json_encoded)
        assert json_decoded.timestamp == current_time
