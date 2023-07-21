import os

import pytest

from opensearchpy import OpenSearch


def is_opensearch__responsive(host, port):
    try:
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ca_certs=False
        )
        status = client.ping()
        client.close()
    except ConnectionError:
        status = False
    return status


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    base_directory = os.path.dirname(os.path.abspath(__file__)).replace("tests", "")
    return os.path.join(base_directory, "docker-compose.yaml")


@pytest.fixture(scope="session")
def opensearch_client(docker_ip, docker_services) -> OpenSearch:
    port = docker_services.port_for("dc-opensearch", 9200)
    docker_services.wait_until_responsive(
        timeout=60.0, pause=20, check=lambda: is_opensearch__responsive(host=docker_ip, port=port)
    )
    return OpenSearch(
        hosts=[{'host': docker_ip, 'port': port}],
        http_auth=("admin", "admin"),
        use_ssl=True,
        verify_certs=False,
        ca_certs=False
    )
