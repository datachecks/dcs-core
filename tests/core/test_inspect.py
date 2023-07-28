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

from datachecks import Configuration, Inspect
from datachecks.core.configuration.configuration import (
    DataSourceConfiguration, DataSourceConnectionConfiguration, DatasourceType,
    MetricConfiguration)
from datachecks.core.logger.default_logger import DefaultLogger
from datachecks.core.metric.base import MetricsType


@pytest.mark.usefixtures(
    "opensearch_client_configuration", "pgsql_connection_configuration"
)
class TestInspect:
    def test_should_invoke_default_logger(
        self,
        opensearch_client_configuration: DataSourceConnectionConfiguration,
        pgsql_connection_configuration: DataSourceConnectionConfiguration,
    ):
        data_source_name = "test_open_search_data_source"
        configuration = Configuration(
            data_sources=[
                DataSourceConfiguration(
                    name=data_source_name,
                    type=DatasourceType.POSTGRES,
                    connection_config=DataSourceConnectionConfiguration(
                        host=pgsql_connection_configuration.host,
                        port=pgsql_connection_configuration.port,
                        database=pgsql_connection_configuration.database,
                        username=pgsql_connection_configuration.username,
                        password=pgsql_connection_configuration.password,
                    ),
                )
            ],
            metrics={
                data_source_name: [
                    MetricConfiguration(
                        name="metric1",
                        metric_type=MetricsType.ROW_COUNT,
                        table="table1",
                    )
                ]
            },
        )
        inspect = Inspect(configuration=configuration)
        assert isinstance(inspect.metric_logger, DefaultLogger)
