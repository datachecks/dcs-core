<p align="center">
    <img alt="Logo" src="https://raw.githubusercontent.com/waterdipai/datachecks/main/docs/assets/datachecks_banner_logo.svg" width="1512">
</p>
<p align="center"><b>Open Source Data Quality Monitoring.</b></p>

<p align="center">
    <img align="center" alt="License" src="https://img.shields.io/badge/License-Apache%202.0-blue.svg"/>
    <img align="center" src="https://img.shields.io/pypi/pyversions/datachecks"/>
    <img align="center" alt="Versions" src="https://img.shields.io/pypi/v/datachecks"/>
    <img align="center" alt="coverage" src="https://static.pepy.tech/personalized-badge/datachecks?period=total&units=international_system&left_color=black&right_color=green&left_text=Downloads"/>
    <img align="center" alt="coverage" src="https://codecov.io/gh/waterdipai/datachecks/branch/main/graph/badge.svg?token=cn6lkDRXpl">
    <img align="center" alt="Status" src="https://github.com/waterdipai/datachecks/actions/workflows/ci.yml/badge.svg?branch=main"/>
</p>

<div align="center">
⭐️ If you like it, star the repo <a href="https://github.com/waterdipai/waterdip/stargazers"></a> ⭐

<h3>|
<a href="https://docs.datachecks.io/">Documentations</a>
|
<a href="https://join.slack.com/t/datachecks/shared_invite/zt-1zqsigy4i-s5aadIh2mjhdpVWU0PstPg">Slack Community</a>
|
</h3>
</div>

## Why Data Monitoring?

APM (Application Performance Monitoring) tools are used to monitor the performance of applications. APM tools are mandatory part of dev stack. Without AMP tools, it is very difficult to monitor the performance of applications.

<p align="center">
    <img alt="why_data_observability" src="https://raw.githubusercontent.com/waterdipai/datachecks/main/docs/assets/datachecks_why_data_observability.svg" width="800">
</p>

But for Data products regular APM tools are not enough. We need a new kind of tools that can monitor the performance of Data applications.
Data monitoring tools are used to monitor the data quality of databases and data pipelines. It identifies potential issues, including in the databases and data pipelines. It helps to identify the root cause of the data quality issues and helps to improve the data quality.

## What is `datachecks`?

Datachecks is an open-source data monitoring tool that helps to monitor the data quality of databases and data pipelines.
It identifies potential issues, including in the databases and data pipelines. It helps to identify the root cause of the data quality issues and helps to improve the data quality.

Datachecks can generate several reliability, uniqueness, completeness metrics from several data sources

### Reports: Data Quality Visualisation

You can generate with just one command. It generates a beautiful data quality report with all the metrics.
This html report can be shared with the team.

<p align="center">
    <img alt="why_data_observability" src="docs/assets/datachecks_dashboard.png" width="800">
</p>

### CLI: Data Quality Visualisation in Bash

Data quality report can be generated in the terminal. It is very useful for debugging. All it takes is one command.

<p align="center">
    <img alt="why_data_observability" src="docs/assets/datachecks_cli_output.png" width="800">
</p>

## Getting Started

Install `datachecks` with the command that is specific to the database.

### Install Datachecks

To install all datachecks dependencies, use the below command.

```shell
pip install datachecks -U
```

### Create the config file

With a simple config file, you can generate data quality reports for your data sources. Below is the sample config example.
For more details, please visit the [config guide](https://docs.datachecks.io/configuration/metric_configuration/)

<p align="center">
    <img alt="why_data_observability" src="docs/assets/datachecks_config.png" width="800">
</p>

### Run from CLI

**Generate Report in Terminal**

```shell
datachecks inspect -C config.yaml
```

**Generate HTML Report**

```shell
datachecks inspect -C config.yaml  --html-report
```

Please visit the [Quick Start Guide](https://docs.datachecks.io/getting_started/)

## Supported Data Sources

Datachecks supports sql and search data sources. Below are the list of supported data sources.

| Data Source                                                             | Type                   | Supported  |
|-------------------------------------------------------------------------|------------------------|------------|
| [Postgres](https://docs.datachecks.io/integrations/postgres/)           | Transactional Database | :thumbsup: |
| [MySql](https://docs.datachecks.io/integrations/mysql/)                 | Transactional Database | :thumbsup: |
| [MS SQL Server](https://docs.datachecks.io/integrations/mssql/)         | Transactional Database | :thumbsup: |
| [OpenSearch](https://docs.datachecks.io/integrations/opensearch/)       | Search Engine          | :thumbsup: |
| [Elasticsearch](https://docs.datachecks.io/integrations/elasticsearch/) | Search Engine          | :thumbsup: |
| [GCP BigQuery](https://docs.datachecks.io/integrations/bigquery/)       | Data Warehouse         | :thumbsup: |
| [DataBricks](https://docs.datachecks.io/integrations/databricks/)       | Data Warehouse         | :thumbsup: |
| [Snowflake](https://docs.datachecks.io/integrations/snowflake/)         | Data Warehouse         | :thumbsup: |
| [AWS RedShift](https://docs.datachecks.io/integrations/redshift/)       | Data Warehouse         | :thumbsup: |

## Metric Types

| Metric                                                                                       | Description                                                                                                      |
| -------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **[Reliability Metrics](https://docs.datachecks.io/metrics/reliability/)**                   | Reliability metrics detect whether tables/indices/collections are updating with timely data                      |
| **[Numeric Distribution Metrics](https://docs.datachecks.io/metrics/numeric_distribution/)** | Numeric Distribution metrics detect changes in the numeric distributions i.e. of values, variance, skew and more |
| **[Uniqueness Metrics](https://docs.datachecks.io/metrics/uniqueness/)**                     | Uniqueness metrics detect when data constraints are breached like duplicates, number of distinct values etc      |
| **[Completeness Metrics](https://docs.datachecks.io/metrics/completeness/)**                 | Completeness metrics detect when there are missing values in datasets i.e. Null, empty value                     |
| **Validity Metrics**                                                                         | Validity metrics detect whether data is formatted correctly and represents a valid value                         |

## Overview

<p align="center">
    <img alt="datacheck_architecture" src="https://raw.githubusercontent.com/waterdipai/datachecks/main/docs/assets/data_check_architecture.svg" width="800">
</p>

## What Datacheck does not do?

<p align="middle">
  <img alt="" src="https://raw.githubusercontent.com/waterdipai/datachecks/main/docs/assets/datachecks_does_not_do.svg" width="800"/>
</p>

## Community & Support

For additional information and help, you can use one of these channels:

- [Slack](https://join.slack.com/t/datachecks/shared_invite/zt-1zqsigy4i-s5aadIh2mjhdpVWU0PstPg) \(Live chat with the team, support, discussions, etc.\)
- [GitHub issues](https://github.com/waterdipai/datachecks/issues) \(Bug reports, feature requests)

## **Contributions**

:raised_hands: We greatly appreciate contributions - be it a bug fix, new feature, or documentation!

Check out the [contributions guide](https://github.com/waterdipai/datachecks/blob/main/CONTRIBUTING.md) and [open issues](https://github.com/waterdipai/datachecks/issues).

**Datachecks contributors: :blue_heart:**

<a href="https://github.com/subhankarb"><img src="https://avatars.githubusercontent.com/u/2178361?v=4" width="50" height="50" alt=""/></a>
<a href="https://github.com/niyasrad"><img src="https://avatars.githubusercontent.com/u/84234554?v=4" width="50" height="50" alt=""/></a>
<a href="https://github.com/WeryZebra-Yue"><img src="https://avatars.githubusercontent.com/u/75676675?v=4" width="50" height="50" alt=""/></a>
<a href="https://github.com/gaurav-wdi"><img src="https://avatars.githubusercontent.com/u/82873511?v=4" width="50" height="50" alt=""/></a>
<a href="https://github.com/PULAK0717"><img src="https://avatars.githubusercontent.com/u/101057457?v=4" width="50" height="50" alt=""/></a>
<a href="https://github.com/fabriciodadosbr"><img src="https://avatars.githubusercontent.com/u/96063978?v=4" width="50" height="50" alt=""/></a>

## Telemetry

[Usage Analytics & Data Privacy](https://github.com/waterdipai/datachecks/blob/main/docs/support/usage_analytics.md)

## License

This project is licensed under the terms of the [APACHE 2 License](https://github.com/waterdipai/datachecks/blob/main/LICENSE).
