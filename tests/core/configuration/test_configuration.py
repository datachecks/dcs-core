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
from datachecks.core.common.models.configuration import DataSourceType
from datachecks.core.configuration.configuration_parser import (
    load_configuration_from_yaml_str,
)


def test_should_read_datasource_config_for_opensearch():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "opensearch"
        connection:
          host: "localhost"
          port: 9200
    metrics:
      - name: test_metric
        metric_type: document_count
        resource: test.index1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.OPENSEARCH


def test_should_read_datasource_config_for_elasticsearch():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "elasticsearch"
        connection:
          host: "localhost"
          port: 9200
    metrics:
      - name: test_metric
        metric_type: document_count
        resource: test.index1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.ELASTICSEARCH


def test_should_read_datasource_config_for_bigquery():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "bigquery"
        connection:
          project: "test-project"
          dataset: "test_dataset"
          credentials_base64: "asda...="
    metrics:
      - name: test_metric
        metric_type: row_count
        resource: test.table1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.BIGQUERY


def test_should_read_datasource_config_for_databricks():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "databricks"
        connection:
          host: "test-project"
          port: 443
          schema: "test_schema"
          catalog: "test_catalog"
          http_path: "sql/protocolv1/o/0/0101-0101010101010101/0101-0101010101010101"
          token: "asda...="
    metrics:
      - name: test_metric
        metric_type: row_count
        resource: test.table1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.DATABRICKS


def test_should_read_datasource_config_for_postgres():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "postgres"
        connection:
          host: "localhost"
          port: 5432
    metrics:
      - name: test_metric
        metric_type: row_count
        resource: test.table1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.POSTGRES


def test_should_read_datasource_config_for_mysql():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "mysql"
        connection:
          host: "localhost"
          port: 3306
          username: "dbuser"
          password: "dbpass"
          database: "dcs_db"
    metrics:
      - name: test_metric
        metric_type: row_count
        resource: test.table1
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.MYSQL


def test_should_read_expression_config():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "postgres"
        connection:
          host: "localhost"
          port: 5432
    metrics:
      - name: test_combined_metric
        metric_type: combined
        expression: mul(1, 2)
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.metrics["test_combined_metric"].metric_type == "combined"
    assert configuration.metrics["test_combined_metric"].expression == "mul(1, 2)"


def test_should_throw_exception_on_invalid_datasource_type():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "invalid"
        connection:
          host: "localhost"
          port: 5432
    metrics:
      - name: test_combined_metric
        metric_type: combined
        expression: mul(1, 2)
    """
    try:
        configuration = load_configuration_from_yaml_str(yaml_string)
    except Exception as e:
        assert (
            str(e)
            == "Failed to parse configuration: 'invalid' is not a valid DataSourceType"
        )
