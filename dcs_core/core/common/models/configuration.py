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
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from markdown_it.rules_block import reference

from dcs_core.core.common.models.data_source_resource import Field, Index, Table
from dcs_core.core.common.models.metric import MetricsType
from dcs_core.core.common.models.validation import (
    Threshold,
    Validation,
    ValidationFunction,
    ValidationFunctionType,
)


class DataSourceType(str, Enum):
    OPENSEARCH = "opensearch"
    ELASTICSEARCH = "elasticsearch"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    BIGQUERY = "bigquery"
    # TEMPORARILY INACTIVE
    # REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    SPARK_DF = "spark_df"
    ORACLE = "oracle"
    DB2 = "db2"
    SYBASE = "sybase"


class DataSourceLanguageSupport(str, Enum):
    SQL = "sql"
    DSL_ES = "dsl_es"


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
    keyfile: Optional[str] = None  # BigQuery specific configuration

    token: Optional[str] = None  # Databricks specific configuration
    catalog: Optional[str] = None  # Databricks specific configuration
    http_path: Optional[str] = None  # Databricks specific configuration

    account: Optional[str] = None  # Snowflake specific configuration
    warehouse: Optional[str] = None  # Snowflake specific configuration
    role: Optional[str] = None  # Snowflake specific configuration

    driver: Optional[str] = None  # SQL Server specific configuration

    spark_session: Optional[Any] = None  # Spark specific configuration

    service_name: Optional[str] = None  # Oracle specific configuration

    security: Optional[str] = None  # IBM DB2 specific configuration
    protocol: Optional[str] = None  # IBM DB2 specific configuration
    server: Optional[str] = None


@dataclass
class DataSourceConfiguration:
    """
    Data source configuration
    """

    name: str
    type: DataSourceType
    connection_config: DataSourceConnectionConfiguration
    language_support: Optional[DataSourceLanguageSupport] = None


@dataclass
class ValidationConfig:
    name: str
    on: str
    threshold: Optional[Threshold] = None
    where: Optional[str] = None
    query: Optional[str] = None
    regex: Optional[str] = None
    values: Optional[List] = None
    ref: Optional[str] = None

    def _ref_field_validation(self):
        if self.ref is not None:
            reference_resources = self.ref.strip().split(".")
            if len(reference_resources) < 2 or len(reference_resources) > 3:
                raise ValueError(
                    "ref field should be in the format of <datasource_name>.<dataset_name>.<field_name>"
                )
            self._ref_data_source_name = reference_resources[0]
            self._ref_dataset_name = reference_resources[1]
            self._ref_field_name = None

            if len(reference_resources) == 3:
                self._ref_field_name = reference_resources[2]

    def _on_field_validation(self):
        if self.on is None:
            raise ValueError("on field is required")
        dataset_validation_functions = [
            ValidationFunction.FAILED_ROWS,
            ValidationFunction.COUNT_ROWS,
            ValidationFunction.COUNT_DOCUMENTS,
            ValidationFunction.CUSTOM_SQL,
            ValidationFunction.DELTA_COUNT_ROWS,
        ]

        if self.on.strip().startswith("delta"):
            self._is_delta_validation = True
            on_statement = re.search(r"^delta\s+(.+)", self.on.strip()).group(1)
        else:
            self._is_delta_validation = False
            on_statement = self.on.strip()

        if on_statement not in dataset_validation_functions:
            self._validation_function_type = ValidationFunctionType.FIELD
            if not re.match(r"^(\w+)\(([ \w-]+)\)$", on_statement):
                raise ValueError(
                    f"on field must be a valid function, was {on_statement}"
                )
            else:
                column_validation_function = re.search(
                    r"^(\w+)\(([ \w-]+)\)$", on_statement
                ).group(1)

                if column_validation_function not in [v for v in ValidationFunction]:
                    raise ValueError(
                        f"{column_validation_function} is not a valid validation function"
                    )

                if column_validation_function in dataset_validation_functions:
                    raise ValueError(
                        f"{column_validation_function} is a table function, should not have column name"
                    )

                self._validation_function = ValidationFunction(
                    column_validation_function
                    if not self._is_delta_validation
                    else f"delta_{column_validation_function}"
                )
                self._validation_field_name = re.search(
                    r"^(\w+)\(([ \w-]+)\)$", on_statement
                ).group(2)
        else:
            self._validation_function_type = ValidationFunctionType.DATASET
            self._validation_function = ValidationFunction(
                on_statement
                if not self._is_delta_validation
                else f"delta_{on_statement}"
            )
            self._validation_field_name = None

    def __post_init__(self):
        self._on_field_validation()
        self._ref_field_validation()

    @property
    def get_validation_function(self) -> ValidationFunction:
        return ValidationFunction(self._validation_function)

    @property
    def get_is_delta_validation(self):
        return self._is_delta_validation

    @property
    def get_ref_data_source_name(self):
        return self._ref_data_source_name if self.ref is not None else None

    @property
    def get_ref_dataset_name(self):
        return self._ref_dataset_name if self.ref is not None else None

    @property
    def get_ref_field_name(self):
        return self._ref_field_name if self.ref is not None else None

    @property
    def get_validation_function_type(self) -> ValidationFunctionType:
        return self._validation_function_type

    @property
    def get_validation_field_name(self) -> str:
        return self._validation_field_name if self._validation_field_name else None


@dataclass
class ValidationConfigByDataset:
    """
    Validation configuration group
    """

    data_source: str
    dataset: str
    validations: Dict[str, ValidationConfig]


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

    data_sources: Optional[Dict[str, DataSourceConfiguration]] = field(
        default_factory=dict
    )
    validations: Optional[Dict[str, ValidationConfigByDataset]] = field(
        default_factory=dict
    )
    metrics: Optional[Dict[str, MetricConfiguration]] = None
    storage: Optional[MetricStorageConfiguration] = None

    def add_spark_session(self, data_source_name: str, spark_session):
        self.data_sources[data_source_name] = DataSourceConfiguration(
            name=data_source_name,
            type=DataSourceType.SPARK_DF,
            connection_config=DataSourceConnectionConfiguration(
                spark_session=spark_session
            ),
        )
