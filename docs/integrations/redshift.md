# **AWS Redshift**

## Install Dependencies

```bash
pip install datachecks[redshift]
```

## Define DataSource Connection in Configuration File

AWS Redshift datasource can be defined as below in the config file.

The type of the data source must be `redshift`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: redshift_datasource
    type: redshift
    config:
      host: <redshift_host>.<region>.redshift.amazonaws.com
      port: <redshift_port>
      username: <redshift_username>
      password: <redshift_password>
      database: <redshift_database>
```
