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
from typing import Dict

from datachecks.core.common.models.metric import MetricsType, MetricValue
from datachecks.core.common.models.profile import TextFieldProfile
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.base import MetricIdentity


class TextSQLFieldProfiler:
    def __init__(
        self,
        data_source: SQLDataSource,
        table_name: str,
        field_name: str,
        data_type: str,
    ):
        self._data_source = data_source
        self._table_name = table_name
        self._field_name = field_name
        self._data_type = data_type

    def generate(self) -> TextFieldProfile:
        data: Dict = self._data_source.profiling_sql_aggregates_string(
            self._table_name, self._field_name
        )
        return self._generate_field_profile(data)

    def _generate_field_profile(self, data: Dict) -> TextFieldProfile:
        """
        Generate a numeric field profile from the data provided.
        """
        profile = TextFieldProfile(
            field_name=self._field_name,
            data_type=self._data_type,
        )
        timestamp = datetime.now(timezone.utc)
        for key, value in data.items():
            metric_value = MetricValue(
                value=value,
                identity=MetricIdentity.generate_identity(
                    metric_name="",
                    metric_type=MetricsType(key),
                    data_source=self._data_source,
                    table_name=self._table_name,
                    field_name=self._field_name,
                ),
                metric_type=MetricsType(key),
                timestamp=timestamp,
                data_source=self._data_source.data_source_name,
                table_name=self._table_name,
                field_name=self._field_name,
            )
            setattr(profile, key, metric_value)
        return profile
