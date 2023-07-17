from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import yaml
from yaml import SafeLoader


@dataclass
class DataSourceConnectionConfiguration:
    """
    Connection configuration for a data source
    """
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    database: Optional[str]
    schema: Optional[str]


@dataclass
class DataSourceConfiguration:
    """
    Data source configuration
    """
    name: str
    type: str
    connection_config: DataSourceConnectionConfiguration


@dataclass
class MetricsPropertiesConfiguration:
    """
    Properties configuration for a metric
    """
    indices: Optional[list]
    tables: Optional[list]


@dataclass
class MetricsFilterConfiguration:
    """
    Filter configuration for a metric
    """
    sql_query: Optional[list]
    search_query: Optional[list]


@dataclass
class MetricConfiguration:
    """
    Metric configuration
    """
    name: str
    type: str
    data_source: str
    properties: Optional[MetricsPropertiesConfiguration]
    filter: Optional[MetricsFilterConfiguration]


@dataclass
class Configuration:
    """
    Configuration for the data checks
    """
    data_sources: List[DataSourceConfiguration]
    metrics: List[MetricConfiguration]


def load_configuration(file_path: str) -> Configuration:
    """
    Load the configuration from a YAML file
    :param file_path:
    :return:
    """
    with open(file_path) as config_yaml_file:
        yaml_string = config_yaml_file.read()
        config_dict: Dict = yaml.safe_load(yaml_string)

        data_source_configurations = [
            DataSourceConfiguration(
                name=data_source["name"],
                type=data_source["type"],
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
        metric_configurations = [MetricConfiguration(
            name=metric["name"],
            type=metric["type"],
            data_source=metric["data_source"],
            properties=MetricsPropertiesConfiguration(
                indices=metric["properties"].get("indices"),
                tables=metric["properties"].get("tables"),
            ),
            filter=MetricsFilterConfiguration(
                sql_query=metric["filter"].get("sql_query"),
                search_query=metric["filter"].get("search_query"),
            ),
        ) for metric in config_dict["metrics"]]
        return Configuration(data_sources=data_source_configurations, metrics=metric_configurations)
