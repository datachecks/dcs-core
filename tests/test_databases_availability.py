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
from opensearchpy import OpenSearch
from sqlalchemy import create_engine, text

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration


@pytest.mark.usefixtures("opensearch_client_config")
def test_opensearch_available(
    opensearch_client_config: DataSourceConnectionConfiguration,
):
    client = OpenSearch(
        hosts=[
            {
                "host": opensearch_client_config.host,
                "port": opensearch_client_config.port,
            }
        ],
        http_auth=(
            opensearch_client_config.username,
            opensearch_client_config.password,
        ),
        use_ssl=True,
        verify_certs=False,
        ca_certs=False,
    )
    assert client.ping()
    client.close()


@pytest.mark.usefixtures("pgsql_connection_configuration")
def test_pgsql_available(
    pgsql_connection_configuration: DataSourceConnectionConfiguration,
):
    host = pgsql_connection_configuration.host
    port = pgsql_connection_configuration.port
    user = pgsql_connection_configuration.username
    password = pgsql_connection_configuration.password
    database = pgsql_connection_configuration.database
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )

    connect = engine.connect()
    assert connect.execute(text("SELECT 1")).fetchone()[0] == 1
    connect.close()
