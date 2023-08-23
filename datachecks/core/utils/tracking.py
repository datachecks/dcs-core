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

"""
Module for anonymous telemetry.

No credentials, data, personal information or anything private is collected (and will never be).
"""
import json
import os
import platform
from time import time
from typing import Optional
from uuid import uuid4

import requests
from loguru import logger

from datachecks.core.utils.utils import truncate_error

TRACKING_DISABLED = os.environ.get("DISABLE_DCS_ANONYMOUS_TELEMETRY", False)
TRACK_URL = "https://hosted.rudderlabs.com/v1/track"
TOKEN = "2U4Bsait5XpEyHnbFtJSjig7KH8"
TIMEOUT = 8

dcs_anonymous_id = None


def is_tracking_enabled():
    return not TRACKING_DISABLED


def get_anonymous_id():
    global dcs_anonymous_id
    if dcs_anonymous_id is None:
        dcs_anonymous_id = str(uuid4())
    return dcs_anonymous_id


def create_error_event(
    exception: Exception,
):
    error = truncate_error(repr(exception))
    return {
        "event": "dcs_error",
        "properties": {
            "distinct_id": get_anonymous_id(),
            "token": TOKEN,
            "time": time(),
            "os_type": os.name,
            "os_version": platform.platform(),
            "python_version": f"{platform.python_version()}/{platform.python_implementation()}",
            "error": error,
        },
    }


def create_inspect_event_json(
    runtime_seconds: float,
    inspect_info: Optional[dict] = None,
    error: Optional[str] = None,
):
    return {
        "event": "dcs_inspect_end",
        "properties": {
            "distinct_id": get_anonymous_id(),
            "token": TOKEN,
            "time": time(),
            "runtime_seconds": runtime_seconds,
            "os_type": os.name,
            "os_version": platform.platform(),
            "python_version": f"{platform.python_version()}/{platform.python_implementation()}",
            "count_metrics": inspect_info.get("metrics_count", 0)
            if inspect_info
            else 0,
            "count_datasource": inspect_info.get("datasource_count", 0)
            if inspect_info
            else 0,
            "count_tables": inspect_info.get("table_count", 0) if inspect_info else 0,
            "count_index": inspect_info.get("index_count", 0) if inspect_info else 0,
            "error": error,
        },
    }


def send_event_json(event_json):
    if is_tracking_enabled():
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic MlU0QnNhaXQ1WHBFeUhuYkZ0SlNqaWc3S0g4Og==",
        }
        data = json.dumps(event_json).encode()
        try:
            response = requests.post(TRACK_URL, data=data, headers=headers)
            if response.status_code != 200:
                raise RuntimeError(response)
        except Exception as e:
            logger.debug(f"Failed to post to Rudderstack: {e}")
