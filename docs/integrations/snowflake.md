# **GCP BigQuery**

## Install Dependencies
```bash
pip install dcs-core[snowflake]
```

## Define DataSource Connection in Configuration File

Snowflake datasource can be defined as below in the config file.

The type of the data source must be `snowflake`.
Note: Ensure sure that the account follows the correct format for the region. For more information, see [Snowflake documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).

Below is an example of the configuration file.

```yaml
data_sources:
  - name: snowflake_datasource
    type: snowflake
    config:
      account: <snowflake account id>
      username: <snowflake username>
      password: <snowflake password>
      database: <snowflake database name>
      schema: <snowflake schema name>
      warehouse: <snowflake warehouse name>
      role: <snowflake role name>
```