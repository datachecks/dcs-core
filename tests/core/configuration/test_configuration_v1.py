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
import pytest

from datachecks.core.common.models.configuration import DataSourceType
from datachecks.core.common.models.validation import ValidationFunction
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
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources["test"].type == DataSourceType.MYSQL


def test_should_parse_dataset_name():
    yaml_string = """
    validations for source.table:
      - test validation:
          on: min(age)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"].validations["test validation"].name
        == "test validation"
    )
    assert (
        configuration.validations["source.table"]
        .validations["test validation"]
        .get_validation_function
        == ValidationFunction.MIN
    )


def test_should_throw_exception_on_invalid_threshold_config():
    yaml_string = """
    validations for source.table:
      - test:
          on: min(age)
          threshold: ">>10"
    """
    with pytest.raises(Exception):
        load_configuration_from_yaml_str(yaml_string)


def test_should_parse_threshold_config():
    yaml_string = """
    validations for source.table:
      - test:
          on: min(age)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"].validations["test"].threshold.gt == 10
    )


def test_should_parse_where_clause():
    yaml_string = """
    validations for source.table:
      - test:
          on: min(age)
          threshold: ">10"
          where: "age > 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"].validations["test"].where
        == "age > 10"
    )


def test_should_parse_query():
    yaml_string = """
    validations for source.table:
      - test:
          on: custom_sql
          threshold: ">10"
          query: "select * from source.table"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"].validations["test"].query
        == "select * from source.table"
    )
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.CUSTOM_SQL
    )


def test_should_parse_regex():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_invalid_regex(species)
          threshold: "<10"
          regex: "(0?[0-9])"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"].validations["test"].regex
        == "(0?[0-9])"
    )
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_INVALID_REGEX
    )


def test_should_parse_values():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_invalid_values(species)
          threshold: "<10"
          values: ['a', 'b', 'c']
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.validations["source.table"].validations["test"].values == [
        "a",
        "b",
        "c",
    ]
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_INVALID_VALUES
    )


def test_parse_failed_rows_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: failed_rows
          query: |
            select * from source.table where age < 20
          threshold: "<10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.FAILED_ROWS
    )
    assert (
        configuration.validations["source.table"].validations["test"].query
        == "select * from source.table where age < 20\n"
    )


def test_should_parse_count_uuid_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_uuid(species)
          threshold: "<10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_UUID
    )


def test_should_parse_percentage_uuid_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_uuid(species)
          threshold: "<10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_UUID
    )


def test_should_parse_count_invalid_values():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_invalid_values(species)
          values: ["versicolor"]
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_INVALID_VALUES
    )


def test_should_parse_percent_invalid_values():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_invalid_values(species)
          values: ["versicolor"]
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_INVALID_VALUES
    )


def test_should_parse_count_valid_values():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_valid_values(species)
          values: ["versicolor"]
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_VALID_VALUES
    )


def test_should_parse_percent_valid_values():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_valid_values(species)
          values: ["versicolor"]
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_VALID_VALUES
    )


def test_should_parse_count_invalid_regex():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_invalid_regex(species)
          regex: ".e.*"
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_INVALID_REGEX
    )


def test_should_parse_percent_invalid_regex():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_invalid_regex(species)
          regex: ".e.*"
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_INVALID_REGEX
    )


def test_should_parse_count_valid_regex():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_valid_regex(species)
          regex: "^(setosa|virginica)$"
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_VALID_REGEX
    )


def test_should_parse_percent_valid_regex():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_valid_regex(species)
          regex: "^(setosa|virginica)$"
          threshold: "< 10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_VALID_REGEX
    )


def test_should_parse_count_usa_phone():
    yaml_string = """
    validations for source.table:
        - test:
            on: count_usa_phone(usa_phone)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_USA_PHONE
    )


def test_should_parse_percent_usa_phone():
    yaml_string = """
    validations for source.table:
        - test:
            on: percent_usa_phone(usa_phone)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_USA_PHONE
    )


def test_should_parse_count_email():
    yaml_string = """
    validations for source.table:
        - test:
            on: count_email(email)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_EMAIL
    )


def test_should_parse_percent_email():
    yaml_string = """
    validations for source.table:
        - test:
            on: percent_email(email)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_EMAIL
    )


def test_should_parse_string_length_min_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: string_length_min(name)
          threshold: ">=5"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.STRING_LENGTH_MIN
    )


def test_should_parse_string_length_max_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: string_length_max(name)
          threshold: "<=100"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.STRING_LENGTH_MAX
    )


def test_should_parse_string_length_avg_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: string_length_average(name)
          threshold: ">=10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.STRING_LENGTH_AVERAGE
    )


def test_should_parse_count_usa_zip_code():
    yaml_string = """
    validations for source.table:
        - test:
            on: count_usa_zip_code(usa_zip_code)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_USA_ZIP_CODE
    )


def test_should_parse_percent_usa_zip_code():
    yaml_string = """
    validations for source.table:
        - test:
            on: percent_usa_zip_code(usa_zip_code)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_USA_ZIP_CODE
    )


def test_should_parse_count_usa_state_code():
    yaml_string = """
    validations for source.table:
        - test:
            on: count_usa_state_code(usa_state_code)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_USA_STATE_CODE
    )


def test_should_parse_percent_usa_state_code():
    yaml_string = """
    validations for source.table:
        - test:
            on: percent_usa_state_code(usa_state_code)
            threshold: "<10"
        """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_USA_STATE_CODE
    )


def test_should_parse_count_latitude_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_latitude(latitude_column)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_LATITUDE
    )


def test_should_parse_percent_latitude_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_latitude(latitude_column)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_LATITUDE
    )


def test_should_parse_count_longitude_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_longitude(longitude_column)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_LONGITUDE
    )


def test_should_parse_percent_longitude_validation():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_longitude(longitude_column)
          threshold: ">10"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_LONGITUDE
    )


def test_should_parse_count_ssn():
    yaml_string = """
    validations for source.table:
      - test:
          on: count_ssn(ssn_number)
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.COUNT_SSN
    )


def test_should_parse_percent_ssn():
    yaml_string = """
    validations for source.table:
      - test:
          on: percent_ssn(ssn_number)
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert (
        configuration.validations["source.table"]
        .validations["test"]
        .get_validation_function
        == ValidationFunction.PERCENT_SSN
    )
