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
from sqlalchemy import text

from datachecks.core.configuration.configuration import (
    DataSourceConnectionConfiguration,
)
from tests.utils import create_opensearch_client, create_postgres_connection


@pytest.mark.usefixtures("opensearch_client_configuration")
def test_opensearch_available(
    opensearch_client_configuration,
):
    client = create_opensearch_client(opensearch_client_configuration)
    assert client.ping()
    client.close()


@pytest.mark.usefixtures("pgsql_connection_configuration")
def test_pgsql_available(
    pgsql_connection_configuration: DataSourceConnectionConfiguration,
):
    psql_connection = create_postgres_connection(pgsql_connection_configuration)

    assert psql_connection.execute(text("SELECT 1")).fetchone()[0] == 1
    psql_connection.close()
