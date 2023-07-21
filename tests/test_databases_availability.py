from opensearchpy import OpenSearch


def test_opensearch_available(opensearch_client: OpenSearch):
    assert opensearch_client.ping()
