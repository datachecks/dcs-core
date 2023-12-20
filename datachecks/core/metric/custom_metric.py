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

from datachecks.core.common.models.metric import MetricsType
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.base import Metric, MetricIdentity


class CustomSqlMetric(Metric):
    def get_metric_identity(self):
        return MetricIdentity.generate_identity(
            metric_type=MetricsType.CUSTOM_SQL,
            metric_name=self.name,
            data_source=self.data_source,
            table_name=self.table_name,
        )

    def _generate_metric_value(self):
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_custom_sql(query=self.custom_sql_query)

        else:
            raise ValueError("Invalid data source type")
