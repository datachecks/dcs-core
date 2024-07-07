# **Completeness Validations**

Completeness Validations play a crucial role in data quality assessment, ensuring your datasets are comprehensive and reliable. By regularly monitoring these validations, you can gain profound insights into the extent to which your data captures the entirety of the intended information. This empowers you to make informed decisions about data integrity and take corrective actions when necessary.

These Validations unveil potential gaps or missing values in your data, enabling proactive data enhancement. Like a well-oiled machine, tracking completeness validations enhances the overall functionality of your data ecosystem. Just as reliability Validations guarantee up-to-date information, completeness Validations guarantee a holistic, accurate dataset.


## **Null Count**

Null count Validations gauge missing data, a crucial aspect of completeness Validations, revealing gaps and potential data quality issues.



**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - null count percentage _in_dataset:
      on: count_null(first_name)
```


## **Null Percentage**

Null percentage Validations reveal missing data, a vital facet of completeness Validations, ensuring data sets are whole and reliable.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - empty_string_percentage_in_dataset:
      on: percent_null(first_name)
```

## **Empty String**

Empty string Validations gauge the extent of missing or null values, exposing gaps that impact data completeness and reliability.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - empty_string_percentage_in_dataset:
      on: count_empty_string(first_name)
```

## **Empty String Percentage**

Empty String Percentage Validations assess data completeness by measuring the proportion of empty strings in datasets.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - empty_string_percentage_in_dataset:
      on: percent_empty_string(first_name)
```