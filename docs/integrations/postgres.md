# **PostgreSQL**

## Create a user

As a super-user, please execute the following SQL commands in order to create a new group, assign a user to that group, and grant necessary permissions to access and monitor system tables.

Please ensure that a secure password is generated and stored properly as it will be used for adding datasource in configuration file

```sql
-- Create user and group
CREATE USER dcs_user WITH PASSWORD 'DBpass123';

CREATE GROUP dcs_group;

ALTER GROUP dcs_group ADD USER dcs_user;

-- Grant Postgres' monitor role to the dcs_group
GRANT pg_monitor TO dcs_group
```


## Granting permissions to tables in a schema
For each schema, execute the following three commands to grant read-only access.
Below is the example for granting access to the public schema.

```sql
-- Grant all permissions to the dcs_group
GRANT USAGE ON SCHEMA "public" TO GROUP dcs_group;

GRANT SELECT ON ALL TABLES IN SCHEMA "public" TO GROUP dcs_group;

ALTER DEFAULT PRIVILEGES IN SCHEMA "public" GRANT SELECT ON TABLES TO GROUP dcs_group;
```


## Define DataSource Connection in Configuration File
Postgresql data source can be defined as below in the config file.

```yaml
# config.yaml
data_sources:
  - name: postgres_datasource
    type: postgres
    config:
      host: localhost
      port: 5432
      user: dbuser
      password: DBpass123
      database: postgres
      schema: public
```