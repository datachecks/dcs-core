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
import glob
from abc import ABC
from pathlib import Path
from typing import Dict, List, TypeVar, Union

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


OUTPUT = TypeVar("OUTPUT")
INPUT = TypeVar("INPUT", Dict, List)


class ConfigParser(ABC):
    def parse(self, config: INPUT) -> OUTPUT:
        raise NotImplementedError


class DataSourceConfigParser(ConfigParser):
    @staticmethod
    def _data_source_connection_config_parser(
        config: Dict,
    ) -> DataSourceConnectionConfiguration:
        connection_config = DataSourceConnectionConfiguration(
            host=config["connection"].get("host"),
            port=config["connection"].get("port"),
            username=config["connection"].get("username"),
            password=config["connection"].get("password"),
            database=config["connection"].get("database"),
            schema=config["connection"].get("schema"),
            project=config["connection"].get("project"),
            dataset=config["connection"].get("dataset"),
            credentials_base64=config["connection"].get("credentials_base64"),
            token=config["connection"].get("token"),
            catalog=config["connection"].get("catalog"),
            http_path=config["connection"].get("http_path"),
            account=config["connection"].get("account"),
            warehouse=config["connection"].get("warehouse"),
            role=config["connection"].get("role"),
        )
        return connection_config

    @staticmethod
    def _check_for_duplicate_names(config_list: List):
        names = []
        for config in config_list:
            if config["name"] in names:
                raise DataChecksConfigurationError(
                    f"Duplicate datasource names found: {config['name']}"
                )
            names.append(config["name"])

    def parse(self, config_list: List[Dict]) -> Dict[str, DataSourceConfiguration]:
        self._check_for_duplicate_names(config_list=config_list)
        data_source_configurations: Dict[str, DataSourceConfiguration] = {}

        for config in config_list:
            name_ = config["name"]
            data_source_configuration = DataSourceConfiguration(
                name=name_,
                type=DataSourceType(config["type"].lower()),
                connection_config=self._data_source_connection_config_parser(
                    config=config
                ),
            )
            data_source_configurations[name_] = data_source_configuration

        return data_source_configurations


class StorageConfigParser(ConfigParser):
    @staticmethod
    def _local_file_storage_config_parser(config: Dict) -> LocalFileStorageParameters:
        if "params" not in config:
            raise DataChecksConfigurationError(
                "storage params should be provided for local file storage configuration"
            )
        if "path" not in config["params"]:
            raise DataChecksConfigurationError(
                "path should be provided for local file storage configuration"
            )
        storage_config = LocalFileStorageParameters(path=config["params"]["path"])

        return storage_config

    def parse(self, config: Dict) -> Union[MetricStorageConfiguration, None]:
        if config["type"] == "local_file":
            storage_config = MetricStorageConfiguration(
                type=MetricStorageType.LOCAL_FILE,
                params=self._local_file_storage_config_parser(config=config),
            )
            return storage_config
        else:
            return None


class MetricsConfigParser(ConfigParser):
    def __init__(self, data_source_configurations: Dict[str, DataSourceConfiguration]):
        self.data_source_configurations = data_source_configurations

    @staticmethod
    def _duplicate_metric_names_check(config: List[Dict]):
        names = []
        for metric_yaml_configuration in config:
            if metric_yaml_configuration["name"] in names:
                raise DataChecksConfigurationError(
                    f"Duplicate metric names found: {metric_yaml_configuration['name']}"
                )
            names.append(metric_yaml_configuration["name"])

    @staticmethod
    def _parse_combined_metric_config(configuration: Dict) -> MetricConfiguration:
        expression_str = configuration["expression"]
        metric_configuration = MetricConfiguration(
            name=configuration["name"],
            metric_type=MetricsType(configuration["metric_type"].lower()),
            expression=expression_str,
        )
        return metric_configuration

    @staticmethod
    def _parse_resource_table(resource_str: str) -> Table:
        splits = resource_str.split(".")
        if len(splits) != 2:
            raise ValueError(f"Invalid resource string {resource_str}")
        return Table(data_source=splits[0], name=splits[1])

    @staticmethod
    def _parse_resource_index(resource_str: str) -> Index:
        splits = resource_str.split(".")
        if len(splits) != 2:
            raise ValueError(f"Invalid resource string {resource_str}")
        return Index(data_source=splits[0], name=splits[1])

    @staticmethod
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
        self,
        resource_str: str,
        data_source_type: DataSourceType,
        metric_type: MetricsType,
    ) -> Union[Table, Index, Field]:
        if data_source_type in [
            DataSourceType.OPENSEARCH,
            DataSourceType.ELASTICSEARCH,
        ]:
            if metric_type in [MetricsType.DOCUMENT_COUNT]:
                return self._parse_resource_index(resource_str)
            else:
                return self._parse_resource_field(resource_str, "index")
        else:
            if metric_type in [MetricsType.ROW_COUNT, MetricsType.CUSTOM_SQL]:
                return self._parse_resource_table(resource_str)
            else:
                return self._parse_resource_field(resource_str, "table")

    def _parse_generic_metric_configuration(
        self, configuration: Dict, metric_type: MetricsType
    ) -> MetricConfiguration:
        resource_str = configuration["resource"]
        data_source_name = resource_str.split(".")[0]

        data_source_configuration: DataSourceConfiguration = (
            self.data_source_configurations[data_source_name]
        )

        metric_configuration = MetricConfiguration(
            name=configuration["name"],
            metric_type=metric_type,
            resource=self._metric_resource_parser(
                resource_str=resource_str,
                data_source_type=data_source_configuration.type,
                metric_type=metric_type,
            ),
            filters=configuration.get("filters"),
        )
        if "filters" in configuration:
            metric_configuration.filter = MetricsFilterConfiguration(
                where=configuration["filters"]["where"]
            )
        if "query" in configuration:
            metric_configuration.query = configuration["query"]

        return metric_configuration

    @staticmethod
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

    def _parse_validation_configuration(self, validation_config: Dict) -> Validation:
        if "threshold" in validation_config:
            threshold = self._parse_threshold_str(
                threshold=validation_config["threshold"]
            )
            return Validation(threshold=threshold)
        else:
            raise DataChecksConfigurationError(
                f"Invalid validation configuration {validation_config}"
            )

    def parse(self, config_list: List[Dict]) -> Dict[str, MetricConfiguration]:
        self._duplicate_metric_names_check(config=config_list)
        metric_configurations: Dict[str, MetricConfiguration] = {}

        for config in config_list:
            metric_type = MetricsType(config["metric_type"].lower())
            if metric_type == MetricsType.COMBINED:
                metric_configuration = self._parse_combined_metric_config(
                    configuration=config
                )
            else:
                metric_configuration = self._parse_generic_metric_configuration(
                    configuration=config, metric_type=metric_type
                )
            if "validation" in config and config["validation"] is not None:
                metric_configuration.validation = self._parse_validation_configuration(
                    config["validation"]
                )
            metric_configurations[metric_configuration.name] = metric_configuration
        return metric_configurations


def _parse_configuration_from_dict(config_dict: Dict) -> Configuration:
    try:
        data_source_configurations = DataSourceConfigParser().parse(
            config_list=config_dict["data_sources"]
        )
        metric_configurations = MetricsConfigParser(
            data_source_configurations=data_source_configurations
        ).parse(config_list=config_dict["metrics"])

        configuration = Configuration(
            data_sources=data_source_configurations, metrics=metric_configurations
        )

        if "storage" in config_dict and config_dict["storage"] is not None:
            configuration.storage = StorageConfigParser().parse(
                config=config_dict["storage"]
            )
        return configuration
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )


def load_configuration_from_yaml_str(yaml_string: str) -> Configuration:
    """
    Load configuration from a yaml string
    """
    try:
        config_dict: Dict = parse_config(data=yaml_string)
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )
    return _parse_configuration_from_dict(config_dict=config_dict)


def load_configuration(configuration_path: str) -> Configuration:
    """
    Load configuration from a yaml file
    :param configuration_path:
    :return:
    """

    path = Path(configuration_path)
    if not path.exists():
        raise DataChecksConfigurationError(
            message=f"Configuration file {configuration_path} does not exist"
        )
    if path.is_file():
        with open(configuration_path) as config_yaml_file:
            yaml_string = config_yaml_file.read()
            return load_configuration_from_yaml_str(yaml_string)
    else:
        config_files = glob.glob(f"{configuration_path}/*.yaml")
        if len(config_files) == 0:
            raise DataChecksConfigurationError(
                message=f"No configuration files found in {configuration_path}"
            )
        else:
            config_dict_list: List[Dict] = []
            for config_file in config_files:
                with open(config_file) as config_yaml_file:
                    yaml_string = config_yaml_file.read()
                    config_dict: Dict = parse_config(data=yaml_string)
                    config_dict_list.append(config_dict)

            final_config_dict = {
                "data_sources": [],
                "metrics": [],
                "storage": None,
            }
            for config_dict in config_dict_list:
                if "data_sources" in config_dict:
                    final_config_dict["data_sources"].extend(
                        config_dict["data_sources"]
                    )
                if "metrics" in config_dict:
                    final_config_dict["metrics"].extend(config_dict["metrics"])
                if "storage" in config_dict:
                    final_config_dict["storage"] = config_dict["storage"]

            return _parse_configuration_from_dict(final_config_dict)
