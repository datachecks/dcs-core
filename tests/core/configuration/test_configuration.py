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

from datachecks.core.configuration.configuration import (
    DatasourceType,
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
      test:
        "test_metric":
          "metric_type": "document_count"
          "index": "test"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources[0].type == DatasourceType.OPENSEARCH


def test_should_read_datasource_config_for_postgres():
    yaml_string = """
    data_sources:
      - name: "test"
        type: "postgres"
        connection:
          host: "localhost"
          port: 5432
    metrics:
      test:
        "test_metric":
          "metric_type": "row_count"
          "table": "test"
    """
    configuration = load_configuration_from_yaml_str(yaml_string)
    assert configuration.data_sources[0].type == DatasourceType.POSTGRES
