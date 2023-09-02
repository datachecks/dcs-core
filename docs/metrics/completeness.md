# **Completeness Metrics**

Completeness metrics play a crucial role in data quality assessment, ensuring your datasets are comprehensive and reliable. By regularly monitoring these metrics, you can gain profound insights into the extent to which your data captures the entirety of the intended information. This empowers you to make informed decisions about data integrity and take corrective actions when necessary.

These metrics unveil potential gaps or missing values in your data, enabling proactive data enhancement. Like a well-oiled machine, tracking completeness metrics enhances the overall functionality of your data ecosystem. Just as reliability metrics guarantee up-to-date information, completeness metrics guarantee a holistic, accurate dataset.


## **Null Count**

Null count metrics gauge missing data, a crucial aspect of completeness metrics, revealing gaps and potential data quality issues.



**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: null_count_in_dataset
      metric_type: null_count
      resource: product_db.products
      field_name: first_name

```


## **Null Percentage**

Null percentage metrics reveal missing data, a vital facet of completeness metrics, ensuring data sets are whole and reliable.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: null_percentage_in_dataset
      metric_type: null_percentage
      resource: product_db.products
      field_name: first_name

```

## **Empty String**

Empty string metrics gauge the extent of missing or null values, exposing gaps that impact data completeness and reliability.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: empty_string_in_dataset
      metric_type: empty_string
      resource: product_db.products
      field_name: first_name

```

## **Empty String Percentage**

Empty String Percentage Metrics assess data completeness by measuring the proportion of empty strings in datasets.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: empty_string_percentage_in_dataset
      metric_type: empty_string_percentage
      resource: product_db.products
      field_name: first_name

```