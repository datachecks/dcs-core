# **Data Source Configuration**

Datachecks will read datasource configuration under the key `datasources` in the configuration file. User can define multiple datasource in the configuration file under `datasources` key.

For example:

```yaml
data_sources:
  - name: product_db
    type: postgres
    connection:
      host: 127.0.0.1
      port: 5421
      username: !ENV ${DB1_USER}
      password: !ENV ${DB1_PASS}
      database: dcs_db
```

## Environment Variables

Datachecks supports environment variables in the configuration file. Environment variables can be used in the configuration file using the syntax `!ENV ${ENV_VARIABLE}`. For example:

```yaml
data_sources:
  - name: product_db
    type: postgres
    connection:
      host: !ENV ${DB_HOST}
```

## Configuration Details

| Parameter       | Mandatory        | Description                                                                                                                                                                          |
|:----------------|:-----------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`          | :material-check: | The name of the datasource. The name should be unique.                                                                                                                               |
| `type`          | :material-check: | The type of the datasource. Possible values are `postgres`, `opensearch` etc. Type of datasource mentioned in each supported datasource documentation                                |
| `connection`    | :material-check: | The connection details of the datasource. The connection details are different for each datasource. The connection details are mentioned in each supported datasource documentation. |