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

from opensearchpy import OpenSearch
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from datachecks.core.common.models.configuration import (
    DataSourceConnectionConfiguration,
)


def is_pgsql_responsive(host, port, username, password, database):
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        )
        connection = engine.connect()
        status = True
        connection.close()
    except ConnectionError:
        status = False
    return status


def is_opensearch_responsive(host, port, username, password):
    try:
        client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,
            ca_certs=False,
        )
        status = client.ping()
        client.close()
    except ConnectionError:
        status = False
    return status


def create_opensearch_client(
    opensearch_client_configuration: DataSourceConnectionConfiguration,
) -> OpenSearch:
    client = OpenSearch(
        hosts=[
            {
                "host": opensearch_client_configuration.host,
                "port": opensearch_client_configuration.port,
            }
        ],
        http_auth=(
            opensearch_client_configuration.username,
            opensearch_client_configuration.password,
        ),
        use_ssl=True,
        verify_certs=False,
        ca_certs=False,
    )
    return client


def create_postgres_connection(
    pgsql_connection_configuration: DataSourceConnectionConfiguration,
) -> Connection:
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{pgsql_connection_configuration.username}:"
        f"{pgsql_connection_configuration.password}@"
        f"{pgsql_connection_configuration.host}:"
        f"{pgsql_connection_configuration.port}/"
        f"{pgsql_connection_configuration.database}",
        future=True,
    )
    connection = engine.connect()
    return connection
