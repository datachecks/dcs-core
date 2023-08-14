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
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from datachecks.core.configuration.configuration import \
    DataSourceConnectionConfiguration
from datachecks.core.datasource.opensearch import \
    OpenSearchSearchIndexDataSource
from datachecks.core.datasource.postgres import PostgresSQLDatasource
from tests.utils import is_opensearch_responsive, is_pgsql_responsive

OS_USER_NAME = "admin"
OS_PASSWORD = "admin"
PSQl_USER_NAME = "postgres"
PSQl_PASSWORD = "postgres"
PSQl_DATABASE = "dc_db"

disable_warnings(InsecureRequestWarning)


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    base_directory = os.path.dirname(os.path.abspath(__file__)).replace("tests", "")
    return os.path.join(base_directory, "docker-compose-test.yaml")


@pytest.fixture(scope="session", autouse=True)
def opensearch_client_configuration(
    docker_ip, docker_services
) -> DataSourceConnectionConfiguration:
    port = docker_services.port_for("test-dc-opensearch", 9200)
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=20,
        check=lambda: is_opensearch_responsive(
            host=docker_ip, port=port, username=OS_USER_NAME, password=OS_PASSWORD
        ),
    )

    return DataSourceConnectionConfiguration(
        host=docker_ip,
        port=port,
        username=OS_PASSWORD,
        password=OS_PASSWORD,
        database=None,
        schema=None,
    )


@pytest.fixture(scope="session", autouse=True)
def pgsql_connection_configuration(
    docker_ip, docker_services
) -> DataSourceConnectionConfiguration:
    port = docker_services.port_for("test-dc-postgres", 5432)
    docker_services.wait_until_responsive(
        timeout=60.0,
        pause=20,
        check=lambda: is_pgsql_responsive(
            host=docker_ip,
            port=port,
            username=PSQl_USER_NAME,
            password=PSQl_PASSWORD,
            database=PSQl_DATABASE,
        ),
    )

    return DataSourceConnectionConfiguration(
        host=docker_ip,
        port=port,
        username=PSQl_USER_NAME,
        password=PSQl_PASSWORD,
        database=PSQl_DATABASE,
        schema="public",
    )


@pytest.fixture(scope="class")
def opensearch_datasource(
    opensearch_client_configuration,
) -> OpenSearchSearchIndexDataSource:
    source = OpenSearchSearchIndexDataSource(
        data_source_name="opensearch",
        data_connection=asdict(opensearch_client_configuration),
    )
    source.connect()
    yield source
    source.close()


@pytest.fixture(scope="class")
def postgres_datasource(
    pgsql_connection_configuration: DataSourceConnectionConfiguration,
) -> PostgresSQLDatasource:
    source = PostgresSQLDatasource(
        data_source_name="postgresql",
        data_source_properties=asdict(pgsql_connection_configuration),
    )
    source.connect()
    yield source
    source.close()
