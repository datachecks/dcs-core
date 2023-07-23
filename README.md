<h1 align="center">Datachecks</h1>
<p align="center"><b>Open Source Data Quality Monitoring.</b></p>

<p align="center">
    <img align="center" alt="License" src="https://img.shields.io/badge/License-Apache%202.0-blue.svg"/>
    <img align="center" src="https://img.shields.io/pypi/pyversions/datachecks"/>
    <img align="center" alt="Versions" src="https://img.shields.io/pypi/v/datachecks"/>
    <img align="center" alt="coverage" src="https://static.pepy.tech/personalized-badge/datachecks?period=total&units=international_system&left_color=black&right_color=green&left_text=Downloads"/>
    <img align="center" alt="coverage" src="https://codecov.io/gh/waterdipai/datachecks/branch/main/graph/badge.svg?token=cn6lkDRXpl">
    <img align="center" alt="Status" src="https://github.com/waterdipai/datachecks/actions/workflows/ci.yml/badge.svg?branch=main"/>
</p>

## What is `datachecks`?

Datachecks is a opensource data quality monitoring tool. It helps to monitor the data quality of the data pipelines. It helps to identify the data quality issues in the databases and  data pipelines.

## Getting Started

**Install `datachecks`**

Install `datachecks` with the command that is specific to the database.

### Postgres

```
pip install datachecks 'datachecks[Postgres]' -U
```

### OpenSearch

```
pip install datachecks 'datachecks[OpenSearch]' -U
```

## Running Datachecks

Datachecks can be run using the command line interface. The command line interface takes the config file as input. The config file contains the data sources and the metrics to be monitored.
```shell
datachecks inspect -C config.yaml
```


## Example Config

### Data Source Configuration

Declare the data sources in the `data_sources` section of the config file.
The data sources can be of type `postgres` or `opensearch`.
```yaml
data_sources:
  - name: search
    type: opensearch
    connection:
      host: 127.0.0.1
      port: 9201
      username: admin
      password: admin
  - name: content
    type: postgres
    connection:
      host: 127.0.0.1
      port: 5431
      username: postgres
      password: changeme
      database: postgres
```

### Metric Configuration

Metrics are defined in the `metrics` section of the config file.

```yaml
metrics:
  content:
    count_content_hat:
      metric_type: row_count
      table: table_1
      filter:
        where_clause: "category = 'HAT' AND is_valid is True"
    count_content_non_valid:
      metric_type: row_count
      table: table_1
      filter:
        where_clause: "is_valid is False"
```