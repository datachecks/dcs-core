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

from dcs_core.core.datasource.sql_datasource import SQLDataSource
from dcs_core.core.validation.base import Validation


class CustomSqlValidation(Validation):
    def _generate_metric_value(self):
        if isinstance(self.data_source, SQLDataSource):
            return self.data_source.query_get_custom_sql(query=self.query)
        else:
            raise ValueError("Invalid data source type")
