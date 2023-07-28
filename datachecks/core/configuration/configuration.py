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
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

import yaml

from datachecks.core.configuration.config_loader import parse_config


class DatasourceType(Enum):
    OPENSEARCH = "opensearch"
    POSTGRES = "postgres"


@dataclass
class DataSourceConnectionConfiguration:
    """
    Connection configuration for a data source
    """

    host: str
    port: int
    database: Optional[str]
    username: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = "public"


@dataclass
class DataSourceConfiguration:
    """
    Data source configuration
    """

    name: str
    type: DatasourceType
    connection_config: DataSourceConnectionConfiguration


@dataclass
class MetricsFilterConfiguration:
    """
    Filter configuration for a metric
    """

    where_clause: Optional[str] = None
    search_query: Optional[str] = None


@dataclass
class MetricConfiguration:
    """
    Metric configuration
    """

    name: str
    metric_type: str
    index: Optional[str] = None
    table: Optional[str] = None
    field: Optional[str] = None
    filters: Optional[MetricsFilterConfiguration] = None


@dataclass
class MetricLoggerConfiguration:
    """
    Configuration for the metric logger
    """

    type: Optional[str] = "default"
    enabled: Optional[bool] = True
    config: Optional[Dict] = None


@dataclass
class Configuration:
    """
    Configuration for the data checks
    """

    data_sources: List[DataSourceConfiguration]
    metrics: Dict[str, List[MetricConfiguration]]
    metric_logger: Optional[MetricLoggerConfiguration] = MetricLoggerConfiguration()


def load_configuration(file_path: str) -> Configuration:
    """
    Load configuration from a yaml file
    :param file_path:
    :return:
    """
    with open(file_path) as config_yaml_file:
        yaml_string = config_yaml_file.read()

        return load_configuration_from_yaml_str(yaml_string)


def load_configuration_from_yaml_str(yaml_string: str) -> Configuration:
    """
    Load configuration from a yaml string
    """

    config_dict: Dict = parse_config(data=yaml_string)
    data_source_configurations = [
        DataSourceConfiguration(
            name=data_source["name"],
            type=DatasourceType(data_source["type"]),
            connection_config=DataSourceConnectionConfiguration(
                host=data_source["connection"]["host"],
                port=data_source["connection"]["port"],
                username=data_source["connection"].get("username"),
                password=data_source["connection"].get("password"),
                database=data_source["connection"].get("database"),
                schema=data_source["connection"].get("schema"),
            ),
        )
        for data_source in config_dict["data_sources"]
    ]

    metric_configurations = {
        data_source_name: [
            MetricConfiguration(
                name=metric_name,
                metric_type=metric_value["metric_type"],
                index=metric_value.get("index"),
                table=metric_value.get("table"),
                field=metric_value.get("field"),
                filters=MetricsFilterConfiguration(
                    where_clause=metric_value.get("filters", {}).get(
                        "where_clause", None
                    ),
                    search_query=metric_value.get("filters", {}).get(
                        "search_query", None
                    ),
                ),
            )
            for metric_name, metric_value in metric_list.items()
        ]
        for data_source_name, metric_list in config_dict["metrics"].items()
    }

    return Configuration(
        data_sources=data_source_configurations, metrics=metric_configurations
    )
