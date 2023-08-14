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

import json
import sys
from typing import Dict

from loguru import logger

from datachecks.core.logger.base import MetricLogger


class DefaultLogger(MetricLogger):
    def __init__(self, **kwargs):
        super().__init__()
        self.time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        self.project_name = "datachecks"

        if "project_name" in kwargs:
            self.project_name = kwargs["project_name"]
        if "time_format" in kwargs:
            self.time_format = kwargs["time_format"]
        # logger.remove()
        logger.add(
            self._loguru_sink_serializer,
            level="INFO",
            enqueue=True,
            serialize=True,
            filter="datachecks.core.logger.default_logger",
        )

    def _loguru_sink_serializer(self, message):
        record = message.record

        simplified = {
            "@timestamp": f"{record['time'].strftime(self.time_format)}",
            "level": record["level"].name,
            "message": record["message"],
            "logger_name": record["name"],
        }
        if self.time_format.endswith("%fZ"):
            simplified[
                "@timestamp"
            ] = f"{record['time'].strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4]}Z"

        if self.project_name is not None:
            simplified["projectName"] = self.project_name

        if record["extra"].get("extra"):
            if "metric_name" in record["extra"]["extra"]:
                simplified["metric_name"] = record["extra"]["extra"]["metric_name"]
            if "metric_value" in record["extra"]["extra"]:
                simplified["metric_value"] = record["extra"]["extra"]["metric_value"]
            if "datasource_name" in record["extra"]["extra"]:
                simplified["datasource_name"] = record["extra"]["extra"][
                    "datasource_name"
                ]
            if "metric_type" in record["extra"]["extra"]:
                simplified["metric_type"] = record["extra"]["extra"]["metric_type"]
            if "identity" in record["extra"]["extra"]:
                simplified["identity"] = record["extra"]["extra"]["identity"]
            if "index_name" in record["extra"]["extra"]:
                simplified["index_name"] = record["extra"]["extra"]["index_name"]
            if "table_name" in record["extra"]["extra"]:
                simplified["table_name"] = record["extra"]["extra"]["table_name"]
            if "field_name" in record["extra"]["extra"]:
                simplified["field_name"] = record["extra"]["extra"]["field_name"]

        serialized = json.dumps(simplified)
        print(serialized, file=sys.stdout)

    def log(
        self, metric_name: str, metric_value: float, metric_tags: Dict[str, str] = None
    ):
        logger_extra_value = {
            "metric_value": metric_value,
            "metric_name": metric_name,
            "datasource_name": metric_tags["dataSourceName"],
            "metric_type": metric_tags["metricType"],
            "identity": metric_tags["identity"],
        }
        if "index_name" in metric_tags and metric_tags["index_name"] is not None:
            logger_extra_value["index_name"] = metric_tags["index_name"]
        elif "table_name" in metric_tags and metric_tags["table_name"] is not None:
            logger_extra_value["table_name"] = metric_tags["table_name"]
        if "field_name" in metric_tags and metric_tags["field_name"] is not None:
            logger_extra_value["field_name"] = metric_tags["field_name"]

        logger.info("Logging metric value", extra={**logger_extra_value})
