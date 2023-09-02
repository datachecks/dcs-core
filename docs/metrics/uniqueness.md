# **Uniqueness Metrics**

Uniqueness metrics play a pivotal role in upholding data quality standards. Just as reliability metrics ensure timely data updates, uniqueness metrics focus on the distinctiveness of data entries within a dataset.

By consistently tracking these metrics, you gain valuable insights into data duplication, redundancy, and accuracy. This knowledge empowers data professionals to make well-informed decisions about data cleansing and optimization strategies. Uniqueness metrics also serve as a radar for potential data quality issues, enabling proactive intervention to prevent major problems down the line.


## **Distinct Count**

 A distinct count metric in data quality measures the number of unique values within a dataset, ensuring accuracy and completeness.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: distinct_count_of_product_categories
      metric_type: distinct_count
      resource: product_db.products
      field_name: product_category
```


## **Duplicate Count**

Duplicate count is a data quality metric that measures the number of identical or highly similar records in a dataset, highlighting potential data redundancy or errors.

**Example**

```yaml title="dcs_config.yaml"

```

