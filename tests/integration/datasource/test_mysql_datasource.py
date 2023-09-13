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
from datachecks.integrations.databases.mysql import MysqlDataSource


def test_should_throw_exception_when_mysql_connect_fail():
    datasource = MysqlDataSource(
        data_source_name="test_mysql_data_source",
        data_connection={
            "username": "dbuser",
            "password": "dbpass",
            "host": "localhost",
            "port": 3306,
            "database": "dcs_db",
        },
    )

    with pytest.raises(DataChecksDataSourcesConnectionError):
        datasource.connect()


def test_should_connect_mysql_datasource():
    datasource = MysqlDataSource(
        data_source_name="test_mysql_data_source",
        data_connection={
            "username": "dbuser",
            "password": "dbpass",
            "host": "localhost",
            "port": 3304,
            "database": "dcs_db",
        },
    )

    datasource.connect()
    assert datasource.is_connected
    datasource.close()
