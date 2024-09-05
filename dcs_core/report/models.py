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

import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


@dataclass
class MetricHealthStatus:
    total_metrics: int
    metric_validation_success: int
    metric_validation_failed: int
    metric_validation_unchecked: int
    health_score: int


@dataclass
class DashboardMetricOverview:
    overall: MetricHealthStatus
    reliability: MetricHealthStatus
    numeric: MetricHealthStatus
    uniqueness: MetricHealthStatus
    completeness: MetricHealthStatus
    custom: MetricHealthStatus


@dataclass
class MetricRow:
    metric_name: str
    data_source: str
    metric_type: str
    is_valid: bool
    metric_value: str
    reason: str

    def get_dict(self):
        return {
            "metric_name": self.metric_name,
            "data_source": "-" if self.data_source is None else self.data_source,
            "metric_type": self.metric_type,
            "is_valid": "-" if self.is_valid is None else str(self.is_valid),
            "metric_value": self.metric_value,
            "reason": "-" if self.reason is None else self.reason,
        }


@dataclass
class DashboardInfo:
    name: str
    metrics: List[MetricRow]
    dashboard: DashboardMetricOverview


class GroupedMetricsType(Enum):
    reliability = ["row_count", "document_count", "freshness"]
    numeric = ["min", "max", "avg", "sum", "stddev", "variance"]
    uniqueness = ["distinct_count", "duplicate_count"]
    completeness = [
        "null_count",
        "null_percentage",
        "empty_string_count",
        "empty_string_percentage",
    ]
    custom = ["combined"]


@dataclass
class TemplateParams:
    dashboard_id: str
    dashboard_info: DashboardInfo
    embed_font: bool = True
    embed_lib: bool = True
    embed_data: bool = True
    font_file: Optional[str] = None
    include_js_files: List[str] = dataclasses.field(default_factory=list)
