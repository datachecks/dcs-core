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

from pyparsing import Combine, Group, Literal
from pyparsing import Optional as OptionalParsing
from pyparsing import Word, delimitedList, nums, oneOf

from datachecks.core.common.errors import DataChecksConfigurationError
from datachecks.core.common.models.configuration import (
    Configuration,
    DataSourceConfiguration,
    DataSourceConnectionConfiguration,
    DataSourceType,
    LocalFileStorageParameters,
    MetricConfiguration,
    MetricsFilterConfiguration,
    MetricStorageConfiguration,
    MetricStorageType,
)
from datachecks.core.common.models.data_source_resource import Field, Index, Table
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.common.models.validation import (
    ConditionType,
    Threshold,
    Validation,
)
from datachecks.core.configuration.config_loader import parse_config

CONDITION_TYPE_MAPPING = {
    ">=": ConditionType.GTE,
    "<=": ConditionType.LTE,
    "=": ConditionType.EQ,
    "<": ConditionType.LT,
    ">": ConditionType.GT,
}


def parse_data_source_yaml_configurations(
    data_source_yaml_configurations: List[Dict],
) -> Dict[str, DataSourceConfiguration]:
    data_sources_names = []
    for data_source_yaml_configuration in data_source_yaml_configurations:
        if data_source_yaml_configuration["name"] in data_sources_names:
            raise DataChecksConfigurationError(
                f"Duplicate datasource names found: {data_source_yaml_configuration['name']}"
            )
        data_sources_names.append(data_source_yaml_configuration["name"])
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


def _parse_threshold_str(threshold: str) -> Threshold:
    try:
        operator = oneOf(">= <= = < >").setParseAction(
            lambda t: CONDITION_TYPE_MAPPING[t[0]]
        )
        number = Combine(
            OptionalParsing(Literal("-"))
            + Word(nums)
            + OptionalParsing(Literal(".") + Word(nums))
        ).setParseAction(lambda t: float(t[0]))

        condition = operator + number
        conditions = delimitedList(
            Group(condition) | Group(condition + Literal("&") + condition),
            delim="&",
        )
        result = conditions.parseString(threshold)
        return Threshold(**{operator: value for operator, value in result})

    except Exception as e:
        raise DataChecksConfigurationError(
            f"Invalid threshold configuration {threshold}: {str(e)}"
        )


def _parse_validation_configuration(validation_config: Dict) -> Validation:
    if "threshold" in validation_config:
        threshold = _parse_threshold_str(threshold=validation_config["threshold"])
        return Validation(threshold=threshold)
    else:
        raise DataChecksConfigurationError(
            f"Invalid validation configuration {validation_config}"
        )


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


def parse_storage_configurations(
    storage_yaml_configurations: Dict,
) -> Union[MetricStorageConfiguration, None]:
    if storage_yaml_configurations["type"] == "local_file":
        if "params" not in storage_yaml_configurations:
            raise DataChecksConfigurationError(
                "storage params should be provided for local file storage configuration"
            )
        if "path" not in storage_yaml_configurations["params"]:
            raise DataChecksConfigurationError(
                "path should be provided for local file storage configuration"
            )
        return MetricStorageConfiguration(
            type=MetricStorageType.LOCAL_FILE,
            params=LocalFileStorageParameters(
                path=storage_yaml_configurations["params"]["path"]
            ),
        )
    else:
        return None


def parse_metric_configurations(
    data_source_configurations: Dict[str, DataSourceConfiguration],
    metric_yaml_configurations: List[Dict],
) -> Dict[str, MetricConfiguration]:
    metric_names = []
    for metric_yaml_configuration in metric_yaml_configurations:
        if metric_yaml_configuration["name"] in metric_names:
            raise DataChecksConfigurationError(
                f"Duplicate metric names found: {metric_yaml_configuration['name']}"
            )
        metric_names.append(metric_yaml_configuration["name"])
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
        if (
            "validation" in metric_yaml_configuration
            and metric_yaml_configuration["validation"] is not None
        ):
            metric_configuration.validation = _parse_validation_configuration(
                metric_yaml_configuration["validation"]
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
        configuration = Configuration(
            data_sources=data_source_configurations, metrics=metric_configurations
        )
        if "storage" in config_dict:
            configuration.storage = parse_storage_configurations(config_dict["storage"])
        return configuration
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )
