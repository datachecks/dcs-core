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

from typing import Dict, List, Union

from pyparsing import Forward, Group, Suppress, Word, alphas, delimitedList, nums

from datachecks.core.common.errors import DataChecksConfigurationError
from datachecks.core.common.models.configuration import (
    Configuration,
    DataSourceConfiguration,
    DataSourceConnectionConfiguration,
    DataSourceType,
    MetricConfiguration,
    MetricsFilterConfiguration,
)
from datachecks.core.common.models.data_source_resource import Field, Index, Table
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.configuration.config_loader import parse_config


def parse_data_source_yaml_configurations(
    data_source_yaml_configurations: List[dict],
) -> Dict[str, DataSourceConfiguration]:
    data_source_configurations: Dict[str, DataSourceConfiguration] = {}
    for data_source_yaml_configuration in data_source_yaml_configurations:
        name_ = data_source_yaml_configuration["name"]
        data_source_configuration = DataSourceConfiguration(
            name=name_,
            type=DataSourceType(data_source_yaml_configuration["type"].lower()),
            connection_config=DataSourceConnectionConfiguration(
                host=data_source_yaml_configuration["connection"].get("host"),
                port=data_source_yaml_configuration["connection"].get("port"),
                username=data_source_yaml_configuration["connection"].get("username"),
                password=data_source_yaml_configuration["connection"].get("password"),
                database=data_source_yaml_configuration["connection"].get("database"),
                schema=data_source_yaml_configuration["connection"].get("schema"),
                project=data_source_yaml_configuration["connection"].get("project"),
                dataset=data_source_yaml_configuration["connection"].get("dataset"),
                credentials_base64=data_source_yaml_configuration["connection"].get(
                    "credentials_base64"
                ),
                token=data_source_yaml_configuration["connection"].get("token"),
                catalog=data_source_yaml_configuration["connection"].get("catalog"),
                http_path=data_source_yaml_configuration["connection"].get("http_path"),
            ),
        )
        data_source_configurations[name_] = data_source_configuration
    return data_source_configurations


def _parse_resource_table(resource_str: str) -> Table:
    splits = resource_str.split(".")
    if len(splits) != 2:
        raise ValueError(f"Invalid resource string {resource_str}")
    return Table(data_source=splits[0], name=splits[1])


def _parse_resource_index(resource_str: str) -> Index:
    splits = resource_str.split(".")
    if len(splits) != 2:
        raise ValueError(f"Invalid resource string {resource_str}")
    return Index(data_source=splits[0], name=splits[1])


def _parse_resource_field(resource_str: str, belongs_to: str) -> Field:
    splits = resource_str.split(".")
    if len(splits) != 3:
        raise ValueError(f"Invalid resource string {resource_str}")
    if belongs_to == "table":
        return Field(
            belongs_to=Table(data_source=splits[0], name=splits[1]), name=splits[2]
        )
    elif belongs_to == "index":
        return Field(
            belongs_to=Index(data_source=splits[0], name=splits[1]), name=splits[2]
        )


def _metric_resource_parser(
    resource_str: str, data_source_type: DataSourceType, metric_type: MetricsType
) -> Union[Table, Index, Field]:
    if data_source_type in [DataSourceType.OPENSEARCH, DataSourceType.ELASTICSEARCH]:
        if metric_type in [MetricsType.DOCUMENT_COUNT]:
            return _parse_resource_index(resource_str)
        else:
            return _parse_resource_field(resource_str, "index")
    else:
        if metric_type in [MetricsType.ROW_COUNT]:
            return _parse_resource_table(resource_str)
        else:
            return _parse_resource_field(resource_str, "table")


def parse_metric_configurations(
    data_source_configurations: Dict[str, DataSourceConfiguration],
    metric_yaml_configurations: List[dict],
) -> Dict[str, MetricConfiguration]:
    metric_configurations: Dict[str, MetricConfiguration] = {}

    for metric_yaml_configuration in metric_yaml_configurations:
        metric_type = MetricsType(metric_yaml_configuration["metric_type"].lower())

        if metric_type == MetricsType.COMBINED:
            expression_str = metric_yaml_configuration["expression"]
            metric_configuration = MetricConfiguration(
                name=metric_yaml_configuration["name"],
                metric_type=MetricsType(
                    metric_yaml_configuration["metric_type"].lower()
                ),
                expression=expression_str,
            )
            metric_configurations[metric_configuration.name] = metric_configuration
        else:
            resource_str = metric_yaml_configuration["resource"]
            data_source_name = resource_str.split(".")[0]
            data_source_configuration: DataSourceConfiguration = (
                data_source_configurations[data_source_name]
            )

            metric_configuration = MetricConfiguration(
                name=metric_yaml_configuration["name"],
                metric_type=MetricsType(
                    metric_yaml_configuration["metric_type"].lower()
                ),
                resource=_metric_resource_parser(
                    resource_str=resource_str,
                    data_source_type=data_source_configuration.type,
                    metric_type=metric_type,
                ),
                filters=metric_yaml_configuration.get("filters"),
            )
            if "filters" in metric_yaml_configuration:
                metric_configuration.filter = MetricsFilterConfiguration(
                    where=metric_yaml_configuration["filters"]["where"]
                )
            metric_configurations[metric_configuration.name] = metric_configuration

    return metric_configurations


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
    try:
        config_dict: Dict = parse_config(data=yaml_string)
        data_source_configurations = parse_data_source_yaml_configurations(
            data_source_yaml_configurations=config_dict["data_sources"]
        )
        metric_configurations = parse_metric_configurations(
            data_source_configurations=data_source_configurations,
            metric_yaml_configurations=config_dict["metrics"],
        )
        return Configuration(
            data_sources=data_source_configurations, metrics=metric_configurations
        )
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )
