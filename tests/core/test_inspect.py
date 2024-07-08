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
from datetime import datetime, timezone
from unittest.mock import Mock

from datachecks.core import Inspect
from datachecks.core.common.models.configuration import (
    Configuration,
    DataSourceConfiguration,
    DataSourceConnectionConfiguration,
    DataSourceType,
    MetricConfiguration,
)
from datachecks.core.common.models.data_source_resource import Table
from datachecks.core.common.models.metric import (
    DataSourceMetrics,
    MetricsType,
    MetricValue,
)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.manager import DataSourceManager

TABLE_NAME = "inspect_metric_test_table"


class TestInspect:
    DATA_SOURCE_NAME = "postgres"

    def test_inspect_should_run_without_auto_profile(self, mocker):
        mock_datasource = Mock(DataSource)

        mock_datasource_manager = Mock(DataSourceManager)
        mock_datasource_manager.get_data_source_names.return_value = [
            self.DATA_SOURCE_NAME
        ]

        mocker.patch(
            "datachecks.core.datasource.manager.DataSourceManager._initialize_data_sources",
        )
        mocker.patch(
            "datachecks.core.datasource.manager.DataSourceManager.get_data_source_names",
            return_value=[self.DATA_SOURCE_NAME],
        )
        mocker.patch(
            "datachecks.core.inspect.Inspect._base_data_source_metrics",
            return_value={
                self.DATA_SOURCE_NAME: DataSourceMetrics(
                    data_source=self.DATA_SOURCE_NAME,
                )
            },
        )
        mocker.patch(
            "datachecks.core.datasource.manager.DataSourceManager.get_data_source",
            return_value=mock_datasource,
        )
        mocker.patch(
            "datachecks.core.inspect.InspectOutput.get_inspect_info",
            return_value={},
        )
        mocker.patch(
            "datachecks.core.metric.base.Metric.get_metric_value",
            return_value=MetricValue(
                identity="test_identity",
                metric_type=MetricsType.ROW_COUNT,
                timestamp=datetime.now(timezone.utc),
                data_source=self.DATA_SOURCE_NAME,
                value=1,
            ),
        )

        configuration = Configuration(
            data_sources={
                self.DATA_SOURCE_NAME: DataSourceConfiguration(
                    name=self.DATA_SOURCE_NAME,
                    type=DataSourceType.POSTGRES,
                    connection_config=DataSourceConnectionConfiguration(
                        host="localhost",
                        port=5432,
                        database="postgres",
                        username="postgres",
                        password="postgres",
                    ),
                )
            },
            metrics={
                "metric1": MetricConfiguration(
                    name="metric1",
                    metric_type=MetricsType.ROW_COUNT,
                    resource=Table(
                        name=TABLE_NAME,
                        data_source=self.DATA_SOURCE_NAME,
                    ),
                )
            },
        )
        inspect = Inspect(configuration=configuration)
        result = inspect.run()
        assert list(result.metrics.keys()) == [self.DATA_SOURCE_NAME]

    def test_inspect_should_throw_exception_on_configuration(self, mocker):
        try:
            metrics = (
                {
                    "metric1": MetricConfiguration(
                        name="metric1",
                        metric_type=MetricsType.ROW_COUNT,
                    )
                },
            )
        except Exception as e:
            assert (
                str(e)
                == "Either expression or resource should be provided for a metric"
            )
