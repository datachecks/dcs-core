# **Mysql**
## Install Dependencies
```bash
pip install datachecks[mysql]
```

## Create a user

As a super-user, please execute the following SQL commands in order to create a new group, assign a user to that group, and grant necessary permissions to access and monitor system tables.

Please ensure that a secure password is generated and stored properly as it will be used for adding datasource in configuration file

```sql

CREATE ROLE dcs_role;
GRANT REFERENCES ON *.* TO dcs_role;
GRANT USAGE ON *.* TO dcs_role;
GRANT SELECT ON *.* TO dcs_role;

CREATE USER dcs_user IDENTIFIED BY 'DBpass123';

GRANT dcs_role to dcs_user WITH ADMIN OPTION;
SET DEFAULT ROLE dcs_role TO dcs_user;
```


## Granting permissions to tables in a schema
For each schema, execute the following three commands to grant read-only access.
Below is the example for granting access to the public schema.

```sql

-- Grant usage on schema and select on current and future child tables
GRANT USAGE ON schema_name.* TO dcs_role;
GRANT SELECT ON schema_name.* TO dcs_role;
GRANT ALL PRIVILEGES ON schema_name.* TO dcs_role;
```


## Define DataSource Connection in Configuration File
Mysql data source can be defined as below in the config file.

```yaml
# config.yaml
data_sources:
  - name: mysql_datasource
    type: mysql
    config:
      host: localhost
      port: 3306
      user: dbuser
      password: DBpass123
      database: dc_db
```