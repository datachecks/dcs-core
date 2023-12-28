# Datacheck Examples

This directory contains examples of how to use the datacheck library. There are two components
- A set of example configuration files
- A helper module to generate example data

## Example Configuration Files

The example configuration files are in the `configurations` directory. They are named `example_*.yaml` and are

## Data Generation Module

This is a tool to generate data for the Datachecks.
It is a command line tool that can be used to generate data for multiple datasource types.

### Running the tool

```shell
# Run the tool from example directory. The same poetry environment can be used for data_generator
$ python -m data_generator --help
```

### Configuration

Copy the example.env file to .env and fill in the values.

### Datasources

Supported datasources are:
- Postgres
- Mysql
- BigQuery
- Databricks
- OpenSearch
- Elasticsearch
- Redshift

### Supported Datasets

Supported Datasets are:
- [Iris](https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv)
- [Dimond](https://ggplot2.tidyverse.org/reference/diamonds.html)
- [Healthexpe](https://ourworldindata.org/grapher/life-expectancy-vs-health-expenditure)
- [SALARY_JOBS](https://raw.githubusercontent.com/plotly/datasets/master/salaries-ai-jobs-net.csv)

### How to load data to postgres
#### Start databases
Run the following command to start the databases
```shell
$ docker-compose up
```

#### Add environment variables
Add environment variables to the .env file
```shell
$ cp example.env .env
```

These are the values for postgres
```
pgsql.host=127.0.0.1
pgsql.port=5421
pgsql.user=postgres
pgsql.pass=postgres
pgsql.database=dcs_db
pgsql.schema=public
```

#### Load data to postgres
Run the following command to load iris to postgres. This command needs to be run from examples directory.
```shell
$ python -m data_generator -d pgsql -ds iris
```

### Datachecks configuration files
The datachecks configuration files are located in the configuration folder.
Postgres configuration files are located in the **configuration/example_postgres_config.yaml** file.
