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

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union

from datachecks.core.common.models.data_source_resource import Field, Index, Table
from datachecks.core.common.models.metric import MetricsType
from datachecks.core.common.models.validation import Validation


class DataSourceType(str, Enum):
    OPENSEARCH = "opensearch"
    ELASTICSEARCH = "elasticsearch"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"


@dataclass
class DataSourceConnectionConfiguration:
    """
    Connection configuration for a data source
    """

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    schema: Optional[str] = None

    project: Optional[str] = None  # BigQuery specific configuration
    dataset: Optional[str] = None  # BigQuery specific configuration
    credentials_base64: Optional[str] = None  # BigQuery specific configuration

    token: Optional[str] = None  # Databricks specific configuration
    catalog: Optional[str] = None  # Databricks specific configuration
    http_path: Optional[str] = None  # Databricks specific configuration

    account: Optional[str] = None  # Snowflake specific configuration
    warehouse: Optional[str] = None  # Snowflake specific configuration
    role: Optional[str] = None  # Snowflake specific configuration

    driver: Optional[str] = None  # SQL Server specific configuration


@dataclass
class DataSourceConfiguration:
    """
    Data source configuration
    """

    name: str
    type: DataSourceType
    connection_config: DataSourceConnectionConfiguration


@dataclass
class MetricsFilterConfiguration:
    """
    Filter configuration for a metric
    """

    where: Optional[str] = None


@dataclass
class MetricConfiguration:
    """
    Metric configuration
    """

    name: str
    metric_type: MetricsType
    expression: Optional[str] = None
    query: Optional[str] = None
    resource: Optional[Union[Table, Index, Field]] = None
    validation: Optional[Validation] = None
    filters: Optional[MetricsFilterConfiguration] = None

    def __post_init__(self):
        if self.expression is None and self.resource is None:
            raise ValueError(
                "Either expression or resource should be provided for a metric"
            )


class MetricStorageType(str, Enum):
    """
    Metric storage type
    """

    LOCAL_FILE = "local_file"


@dataclass
class LocalFileStorageParameters:
    """
    Local file metric storage parameters
    """

    path: str


@dataclass
class MetricStorageConfiguration:
    """
    Metric storage configuration
    """

    type: MetricStorageType
    params: Union[LocalFileStorageParameters]


@dataclass
class Configuration:
    """
    Configuration for the data checks
    """

    data_sources: Dict[str, DataSourceConfiguration]
    metrics: Dict[str, MetricConfiguration]
    storage: Optional[MetricStorageConfiguration] = None
