# **Databricks**

## Install Dependencies
```bash
pip install dcs-core[databricks]
```

## Define DataSource Connection in Configuration File

Databricks datasource can be defined as below in the config file.

The type of the data source must be `databricks`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: databricks_datasource
    type: databricks
    config:
      host: "<>.cloud.databricks.com"
      port: 443
      schema: "test_schema"
      catalog: "test_catalog"
      http_path: "sql/1.0/warehouses/..."
      token: "36 char token generated in Databricks"
```