# **OpenSearch**

## Install Dependencies
```bash
pip install datachecks[opensearch]
```

## Define DataSource Connection in Configuration File

OpenSearch data source can be defined as below in the config file.

The type of the data source must be `opensearch`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: opensearch_datasource
    type: opensearch
    config:
      host: localhost
      port: 9200
      username: admin
      password: changeme
```