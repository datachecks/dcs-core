# **Reliability Validations**

Reliability Validations are an essential tool for ensuring that your tables, indices, or collections are being updated with the most up-to-date and timely data.

By consistently monitoring these validations, you can gain better insights into how your systems are performing and make more informed decisions about how to optimize and improve performance. Additionally, these validations can help you identify any potential issues or bottlenecks in your data pipelines, allowing you to take proactive steps to address them before they become major problems.

Overall, investing in a reliable and robust set of validations is crucial for maintaining the health and performance of your data applications and ensuring that your systems are running as smoothly and efficiently as possible.


## **Freshness**

Data freshness, also known as data timeliness, refers to the frequency at which data is updated for consumption. It is an important dimension of data quality and a pillar of data observability because recently updated data is more accurate and, therefore, more valuable.

In the below example the validation will look for the last updated timestamp of the table or index using `updated_at` field.

The threshold will trigger a validation error when the validation is greater than 86400 seconds

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - freshness_of_products:
      on: freshness(updated_at)
      threshold: "> 86400"
```


## **Row Count**

The row count validation determines the total number of rows present in a table.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count of products:
      on: count_rows
      where: "country_code = 'IN'"
      threshold: "> 1000"
```

## **Document Count**

The document count validation determines the total number of documents present in a search data source index.

**Example**

```yaml title="dcs_config.yaml"
validations for search_datastore.product_data_index:
  - count of documents:
      on: count_documents
      threshold: "> 1000"
```
