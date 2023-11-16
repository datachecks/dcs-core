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

from datachecks.core.common.errors import DataChecksDataSourcesConnectionError
from datachecks.integrations.databases.postgres import PostgresDataSource


def test_should_throw_exception_when_postgres_connect_fail():
    datasource = PostgresDataSource(
        data_source_name="test_postgres_data_source",
        data_connection={
            "username": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 2000,
            "database": "postgres",
        },
    )

    with pytest.raises(DataChecksDataSourcesConnectionError):
        datasource.connect()
