# **GCP BigQuery**

## Install Dependencies
```bash
pip install datachecks[bigquery]
```

## Define DataSource Connection in Configuration File

BigQuery datasource can be defined as below in the config file.

The type of the data source must be `bigquery`.

Below is an example of the configuration file.

```yaml
data_sources:
  - name: bigquery_datasource
    type: bigquery
    config:
      project: <gcp_project_id>
      dataset: <gcp_dataset_name>
      credentials_base64: <base64 encoded credentials json>
```

### How to create base64 encoded credentials json?

To create the base64 encoded string you can use the command line tool `base64`, or `openssl base64`, or `python -m base64`