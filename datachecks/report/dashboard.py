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

import base64
import json
import os
import uuid
from dataclasses import asdict

from datachecks.core.common.models.metric import DataSourceMetrics
from datachecks.core.inspect import InspectOutput
from datachecks.report.models import (
    DashboardInfo,
    DashboardMetricOverview,
    GroupedMetricsType,
    MetricHealthStatus,
    MetricRow,
    TemplateParams,
)

STATIC_PATH = os.path.join(os.path.dirname(__file__), "static")


def __load_js():
    return open(f"{STATIC_PATH}/index.js", encoding="utf-8").read()


def __load_font(font: str):
    return base64.b64encode(
        open(f"{STATIC_PATH}/assets/fonts/{font}", "rb").read()
    ).decode()


def __load_image(image: str):
    return base64.b64encode(
        open(f"{STATIC_PATH}/assets/images/{image}", "rb").read()
    ).decode()


def dashboard_info_to_json(dashboard_info: DashboardInfo):
    asdict_result = asdict(dashboard_info)
    return json.dumps(asdict_result)


def html_template(params: TemplateParams):
    lib_block = (
        f"""<script>{__load_js()}</script>"""
        if params.embed_lib
        else "<!-- no embedded lib -->"
    )
    data_block = (
        f"""<script>
            var data_{params.dashboard_id} = {dashboard_info_to_json(params.dashboard_info)};
        </script>"""
        if params.embed_data
        else "<!-- no embedded data -->"
    )
    js_files_block = "\n".join(
        [f'<script src="{file}"></script>' for file in params.include_js_files]
    )
    return f"""
        <html>
            <head>
                <meta charset="utf-8">
                    <style>
                    /* fallback */
                    @font-face {{
                      font-family: "DMSans";
                      font-weight: 600;
                      src: url(data:font/ttf;base64,{__load_font("DMSans-SemiBold.ttf")}) format("truetype");
                    }}
                    @font-face {{
                      font-family: "DMSans";
                      font-weight: 400;
                      src: url(data:font/ttf;base64,{__load_font("DMSans-Regular.ttf")}) format("truetype");
                    }}
                    @font-face {{
                      font-family: "DMSans";
                      font-weight: 500;
                      src: url(data:font/ttf;base64,{__load_font("DMSans-Medium.ttf")}) format("truetype");
                    }}
                    @font-face {{
                      font-family: "DMSans";
                      font-weight: 700;
                      src: url(data:font/ttf;base64,{__load_font("DMSans-Bold.ttf")}) format("truetype");
                    }}
                    .datachecks_logo {{
                        background-image: url(data:image/svg+xml;base64,{__load_image("logo.svg")});
                        background-repeat: no-repeat;
                        background-size: contain;
                    }}
                    .github_logo {{
                        background-image: url(data:image/svg+xml;base64,{__load_image("github.svg")});
                        background-repeat: no-repeat;
                        background-size: contain;
                        filter: invert(1);
                    }}
                    .slack_logo {{
                        background-image: url(data:image/svg+xml;base64,{__load_image("slack.svg")});
                        background-repeat: no-repeat;
                        background-size: contain;
                        filter: invert(1);

                    }}
                    .docs_logo {{
                        background-image: url(data:image/svg+xml;base64,{__load_image("docs.svg")});
                        background-repeat: no-repeat;
                        background-size: contain;
                        filter: invert(1);
                    }}

                    </style>
                    {data_block}
            </head>
        <body>
            <div id="root_{params.dashboard_id}">Loading...</div>
            {lib_block}
            {js_files_block}
            <script>
                window.buildDashboard(data_{params.dashboard_id},
                    "root_{params.dashboard_id}"
                );
            </script>
        </body>
    </html>
    """


class DashboardInfoBuilder:
    def __init__(self, inspect_data: InspectOutput):
        self.inspect_data: InspectOutput = inspect_data

    def build(self):
        [dashboard, metrics] = self.__build_params()
        return DashboardInfo(name="test", metrics=metrics, dashboard=dashboard)

    def __build_params(self):
        data = self.inspect_data

        self.metrics = []
        self.dashboard = DashboardMetricOverview(
            **{
                metric_type: MetricHealthStatus(
                    total_metrics=0,
                    metric_validation_success=0,
                    metric_validation_failed=0,
                    metric_validation_unchecked=0,
                    health_score=0,
                )
                for metric_type in DashboardMetricOverview.__dataclass_fields__.keys()
            }
        )
        for data_source_name, ds_metrics in data.metrics.items():
            row = None
            if isinstance(ds_metrics, DataSourceMetrics):
                for tabel_name, table_metrics in ds_metrics.table_metrics.items():
                    for metric_identifier, metric in table_metrics.metrics.items():
                        self._insert_value(metric)

                for index_name, index_metrics in ds_metrics.index_metrics.items():
                    for metric_identifier, metric in index_metrics.metrics.items():
                        self._insert_value(metric)
            else:
                for metric_identifier, metric in ds_metrics.metrics.items():
                    self._insert_value(metric)
        self.calculate_health_score()
        return [self.dashboard, self.metrics]

    def _insert_value(self, metric):
        for metric_type in GroupedMetricsType.__members__.keys():
            if metric.metric_type in GroupedMetricsType[metric_type].value:
                current = getattr(self.dashboard, metric_type)
                overall = getattr(self.dashboard, "overall")
                current.total_metrics += 1
                overall.total_metrics += 1
                if metric.is_valid is not None:
                    if metric.is_valid:
                        current.metric_validation_success += 1
                        overall.metric_validation_success += 1
                    else:
                        current.metric_validation_failed += 1
                        overall.metric_validation_failed += 1
                else:
                    current.metric_validation_unchecked += 1
                    overall.metric_validation_unchecked += 1
        self.metrics.append(
            MetricRow(
                metric_name=metric.tags.get("metric_name"),
                data_source=metric.data_source,
                metric_type=metric.metric_type,
                metric_value=f"{metric.value:.2f}",
                is_valid=metric.is_valid,
                reason=metric.reason,
            ).get_dict()
        )

    def calculate_health_score(self):
        for metric_type in DashboardMetricOverview.__dataclass_fields__.keys():
            try:
                metric = getattr(self.dashboard, metric_type)
                metric.health_score = round(
                    (
                        metric.metric_validation_success
                        / (
                            metric.metric_validation_success
                            + metric.metric_validation_failed
                        )
                    )
                    * 100,
                    2,
                )
            except ZeroDivisionError:
                metric.health_score = 0
