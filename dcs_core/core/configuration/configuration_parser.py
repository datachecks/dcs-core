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
import re
from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional, TypeVar, Union

from pyparsing import Combine, Group, Literal
from pyparsing import Optional as OptionalParsing
from pyparsing import Word, delimitedList, nums, oneOf

from dcs_core.core.common.errors import DataChecksConfigurationError
from dcs_core.core.common.models.configuration import (
    Configuration,
    DataSourceConfiguration,
    DataSourceConnectionConfiguration,
    DataSourceLanguageSupport,
    DataSourceType,
    ValidationConfig,
    ValidationConfigByDataset,
)
from dcs_core.core.common.models.data_source_resource import Field, Index, Table
from dcs_core.core.common.models.metric import MetricsType
from dcs_core.core.common.models.validation import ConditionType, Threshold, Validation
from dcs_core.core.configuration.config_loader import parse_config

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
            keyfile=config["connection"].get("keyfile"),
            token=config["connection"].get("token"),
            catalog=config["connection"].get("catalog"),
            http_path=config["connection"].get("http_path"),
            account=config["connection"].get("account"),
            warehouse=config["connection"].get("warehouse"),
            role=config["connection"].get("role"),
            service_name=config["connection"].get("service_name"),
            security=config["connection"].get("security"),
            protocol=config["connection"].get("protocol"),
            driver=config["connection"].get("driver"),
            server=config["connection"].get("server"),
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
            data_source_type = DataSourceType(config["type"].lower())
            if data_source_type in [
                DataSourceType.ELASTICSEARCH,
                DataSourceType.OPENSEARCH,
            ]:
                language_support = DataSourceLanguageSupport.DSL_ES
            else:
                language_support = DataSourceLanguageSupport.SQL
            data_source_configuration = DataSourceConfiguration(
                name=name_,
                type=DataSourceType(config["type"].lower()),
                connection_config=self._data_source_connection_config_parser(
                    config=config
                ),
                language_support=language_support,
            )
            data_source_configurations[name_] = data_source_configuration

        return data_source_configurations


class ValidationConfigParser(ConfigParser):
    def parse(self, config: Dict) -> Dict[str, ValidationConfigByDataset]:
        validation_group: Dict[str, ValidationConfigByDataset] = {}
        for key, validations in config.items():
            match = re.search(r"^(validations for)\s([ \w-]+)\.([ \w-]+)$", key)
            if match:
                data_source, dataset = match.group(2), match.group(3)
                validation_dict = {}
                for validation in validations:
                    if not isinstance(validation, dict):
                        raise DataChecksConfigurationError(
                            message=f"Validation must be a dictionary"
                        )
                    if len(validation) != 1:
                        raise DataChecksConfigurationError(
                            message=f"Validation must have only one name"
                        )
                    validation_name, value = next(iter(validation.items()))

                    validation_config = ValidationConfig(
                        name=validation_name,
                        on=value.get("on"),
                        threshold=(
                            self._parse_threshold_str(value.get("threshold"))
                            if value.get("threshold")
                            else None
                        ),
                        where=value.get("where"),
                        query=value.get("query"),
                        regex=value.get("regex"),
                        values=value.get("values"),
                        ref=value.get("ref"),
                    )
                    validation_dict[validation_name] = validation_config

                validation_group[
                    f"{data_source}.{dataset}"
                ] = ValidationConfigByDataset(
                    data_source=data_source,
                    dataset=dataset,
                    validations=validation_dict,
                )
        return validation_group

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


def _parse_configuration_from_dict(config_dict: Dict) -> Configuration:
    try:
        data_source_configurations = {}
        if "data_sources" in config_dict:
            data_source_configurations = DataSourceConfigParser().parse(
                config_list=config_dict["data_sources"]
            )
        validate_configurations = ValidationConfigParser().parse(config_dict)

        configuration = Configuration(
            data_sources=data_source_configurations, validations=validate_configurations
        )

        return configuration
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )


def load_configuration_from_yaml_str(
    yaml_string: str, configuration: Optional[Configuration] = None
) -> Configuration:
    """
    Load configuration from a yaml string
    """
    try:
        config_dict: Dict = parse_config(data=yaml_string)
    except Exception as ex:
        raise DataChecksConfigurationError(
            message=f"Failed to parse configuration: {str(ex)}"
        )
    from_dict = _parse_configuration_from_dict(config_dict=config_dict)
    if configuration:
        for k, v in from_dict.data_sources.items():
            configuration.data_sources[k] = v
        for k, v in from_dict.validations.items():
            configuration.validations[k] = v
    return from_dict


def load_configuration(
    configuration_path: str, configuration: Optional[Configuration] = None
) -> Configuration:
    """
    Load configuration from a yaml file
    :param configuration_path: Configuration file path
    :param configuration: Configuration
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
            return load_configuration_from_yaml_str(
                yaml_string, configuration=configuration
            )
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

                for key, value in config_dict.items():
                    if key not in ["data_sources", "metrics", "storage"]:
                        if key not in final_config_dict.keys():
                            final_config_dict[key] = value
                        else:
                            if isinstance(final_config_dict[key], list):
                                final_config_dict[key].extend(value)

            from_dict = _parse_configuration_from_dict(final_config_dict)
            if configuration:
                for k, v in from_dict.data_sources.items():
                    configuration.data_sources[k] = v
                for k, v in from_dict.validations.items():
                    configuration.validations[k] = v

            return from_dict
