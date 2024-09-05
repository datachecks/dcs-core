# **MS SQL Server**
## Install Dependencies
```bash
pip install dcs-core[mssql]
```


## Define DataSource Connection in Configuration File
MS SQL Server data source can be defined as below in the config file.

```yaml
# config.yaml
data_sources:
  - name: mssql_datasource
    type: mssql
    config:
      host: test.database.windows.net
      port: 1433
      username: dbuser
      password: DBpass123
      database: test-dcs
      schema: dbo
      driver: ODBC Driver 17 for SQL Server
```