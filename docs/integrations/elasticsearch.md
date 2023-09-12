# **ElasticSearch**

## Install Dependencies
```bash
pip install datachecks[elasticsearch]
```

## Define DataSource Connection in Configuration File

ElasticSearch data source can be defined as below in the config file.

The type of the data source must be `elasticsearch`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: elasticsearch_datasource
    type: elasticsearch
    config:
      host: localhost
      port: 9200
      username: admin|optional
      password: changeme|optional
```