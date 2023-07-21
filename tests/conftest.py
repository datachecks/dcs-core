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

import os
from dataclasses import asdict

import pytest
from opensearchpy import OpenSearch
from sqlalchemy import Connection, create_engine

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration
from datachecks.core.datasource.opensearch import \
    OpenSearchSearchIndexDataSource


def is_opensearch_responsive(host, port):
    try:
        client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ca_certs=False,
        )
        status = client.ping()
        client.close()
    except ConnectionError:
        status = False
    return status


def is_pgsql_responsive(host, port):
    try:
        engine = create_engine(
            f"postgresql+psycopg2://dbuser:dbpass@{host}:{port}/postgres"
        )
        connection = engine.connect()
        status = True
        connection.close()
    except ConnectionError:
        status = False
    return status


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    base_directory = os.path.dirname(os.path.abspath(__file__)).replace("tests", "")
    return os.path.join(base_directory, "docker-compose.yaml")


@pytest.fixture(scope="session")
def opensearch_client_config(
    docker_ip, docker_services
) -> DataSourceConnectionConfiguration:
    port = docker_services.port_for("dc-opensearch", 9200)
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=20,
        check=lambda: is_opensearch_responsive(host=docker_ip, port=port),
    )

    return DataSourceConnectionConfiguration(
        host=docker_ip,
        port=port,
        username="admin",
        password="admin",
        database=None,
        schema=None,
    )


@pytest.fixture(scope="session")
def pgsql_connection_configuration(
    docker_ip, docker_services
) -> DataSourceConnectionConfiguration:
    port = docker_services.port_for("dc-postgres", 5432)
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=20,
        check=lambda: is_pgsql_responsive(host=docker_ip, port=port),
    )

    return DataSourceConnectionConfiguration(
        host=docker_ip,
        port=port,
        username="dbuser",
        password="dbpass",
        database="postgres",
        schema="public",
    )


@pytest.mark.usefixtures("opensearch_client_config")
@pytest.fixture(scope="session")
def opensearch_client(
    opensearch_client_config: DataSourceConnectionConfiguration,
) -> OpenSearch:
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
    return client


@pytest.mark.usefixtures("opensearch_client_config")
@pytest.fixture(scope="session")
def opensearch_datasource(
    opensearch_client_config: DataSourceConnectionConfiguration,
) -> OpenSearchSearchIndexDataSource:
    source = OpenSearchSearchIndexDataSource(
        data_source_name="opensearch", data_connection=asdict(opensearch_client_config)
    )
    source.connect()
    return source
