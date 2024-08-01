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