# **Uniqueness Validations**

Uniqueness Validations play a pivotal role in upholding data quality standards. Just as reliability Validations ensure timely data updates, uniqueness Validations focus on the distinctiveness of data entries within a dataset.

By consistently tracking these Validations, you gain valuable insights into data duplication, redundancy, and accuracy. This knowledge empowers data professionals to make well-informed decisions about data cleansing and optimization strategies. Uniqueness Validations also serve as a radar for potential data quality issues, enabling proactive intervention to prevent major problems down the line.


## **Distinct Count**

 A distinct count Validation in data quality measures the number of unique values within a dataset, ensuring accuracy and completeness.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - distinct_count_of_product_categories:
      on: count_distinct(product_category)
```


## **Duplicate Count**

Duplicate count is a data quality Validation that measures the number of identical or highly similar records in a dataset, highlighting potential data redundancy or errors.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - distinct count of product categories:
      on: count_duplicate(product_category)
```