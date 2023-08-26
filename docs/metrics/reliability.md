# **Reliability Metrics**

Reliability metrics are an essential tool for ensuring that your tables, indices, or collections are being updated with the most up-to-date and timely data.

By consistently monitoring these metrics, you can gain better insights into how your systems are performing and make more informed decisions about how to optimize and improve performance. Additionally, these metrics can help you identify any potential issues or bottlenecks in your data pipelines, allowing you to take proactive steps to address them before they become major problems.

Overall, investing in a reliable and robust set of metrics is crucial for maintaining the health and performance of your data applications and ensuring that your systems are running as smoothly and efficiently as possible.


## **Freshness**

Data freshness, also known as data timeliness, refers to the frequency at which data is updated for consumption. It is an important dimension of data quality and a pillar of data observability because recently updated data is more accurate and, therefore, more valuable.

The resource name of freshness metric should be in the format `<datasource>.<table_name>.<timestamp_field>` or `<datasource>.<index_name>.<timestamp_field>`.

In the below example the metric will look for the last updated timestamp of the table or index using `updated_at` field.

**Example**

```yaml title="dcs_config.yaml"
metrics:
  - name: freshness_of_products
    metric_type: freshness
    resource: product_db.products.updated_at
```


## **Row Count**

The row count metric determines the total number of rows present in a table.

**Example**

```yaml title="dcs_config.yaml"
metrics:
  - name: count_of_products
    metric_type: row_count
    resource: product_db.products
    filters:
      where: "country_code = 'IN'"
```

## **Document Count**

The document count metric determines the total number of documents present in a search data source index.

**Example**

```yaml title="dcs_config.yaml"
metrics:
  - name: count_of_documents
    metric_type: document_count
    resource: search_datastore.product_data_index
```