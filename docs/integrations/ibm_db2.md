# **IBM DB2**

## Install Dependencies
```bash
pip install dcs-core[db2]
```

## Define DataSource Connection in Configuration File

IBM DB2 datasource can be defined as below in the config file.

The type of the data source must be `db2`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: db2_datasource
    type: db2
    connection:
      host: localhost
      port: 50000
      username: username
      password: password
      database: bludb
      security: ssl
      protocol: TCPIP
      schema: DCS
```