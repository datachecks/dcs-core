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

from datachecks.core.inspect import InspectOutput
from datachecks.report.models import DashboardInfo, TemplateParams

STATIC_PATH = os.path.join(os.path.dirname(__file__), "static")
print(STATIC_PATH)


def __load_js():
    return open(f"{STATIC_PATH}/index.js", encoding="utf-8").read()


def __load_font():
    return base64.b64encode(
        open(os.path.join(STATIC_PATH, "material-ui-icons.woff2"), "rb").read()
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
                      font-family: 'Material Icons';
                      font-style: normal;
                      font-weight: 400;
                      src: {f"url(data:font/ttf;base64,{__load_font()}) format('woff2');" if params.embed_font else
                        f"url({params.font_file});"}
                    }}

                    .material-icons {{
                      font-family: 'Material Icons';
                      font-weight: normal;
                      font-style: normal;
                      font-size: 24px;
                      line-height: 1;
                      letter-spacing: normal;
                      text-transform: none;
                      display: inline-block;
                      white-space: nowrap;
                      word-wrap: normal;
                      direction: ltr;
                      text-rendering: optimizeLegibility;
                      -webkit-font-smoothing: antialiased;
                    }}
                    </style>
                    {data_block}
            </head>
        <body>
            <div id="root_{params.dashboard_id}">Loading...</div>
            {lib_block}
            {js_files_block}
            <script>
                window.buildDashboard("data_{params.dashboard_id}",
                    "root_{params.dashboard_id}"
                );
            </script>
        </body>
        """


class DashboardInfoBuilder:
    def __init__(self, inspect_data: InspectOutput):
        self.inspect_data: InspectOutput = inspect_data

    def build(self):
        return DashboardInfo("Report")
