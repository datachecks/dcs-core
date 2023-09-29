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
from typing import List, Optional


@dataclass
class MetricHealthStatus:
    total_metrics: int
    metric_validation_success: int
    metric_validation_failed: int
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
class DashboardInfo:
    name: str
    dashboard_metric_overview: DashboardMetricOverview


@dataclass
class TemplateParams:
    dashboard_id: str
    dashboard_info: DashboardInfo
    embed_font: bool = True
    embed_lib: bool = True
    embed_data: bool = True
    font_file: Optional[str] = None
    include_js_files: List[str] = dataclasses.field(default_factory=list)
