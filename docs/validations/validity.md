# **Validity Validations**

Validity checks ensure data is not only correctly formatted but also valid. These metrics are crucial for data quality assurance by verifying adherence to predefined rules and standards. Implementing these checks helps users detect and fix errors, maintaining data integrity and reliability.

## Count UUID

The count UUID validation checks the number of UUIDs in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count uuid for product_id:
      on: count_uuid(product_id)
      threshold: "> 100"
```

## Percentage UUID

The percentage UUID validation checks the percentage of UUIDs in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percentage uuid for product_id:
      on: percent_uuid(product_id)
      threshold: "> 90"
```
## String Length Max

The StringLengthMaxValidation checks the maximum length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name max length:
      on: string_length_max(product_name)
      threshold: "<= 100"
```
## String Length Min

The StringLengthMinValidation checks the minimum length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name min length:
      on: string_length_min(product_name)
      threshold: ">= 5"
```
## String Length Average

The StringLengthAverageValidation checks the average length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name average length:
      on: string_length_average(product_name)
      threshold: ">= 10"
```
