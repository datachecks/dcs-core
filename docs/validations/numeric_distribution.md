# **Numeric Distribution Validations**

Numeric Distribution Validations detect changes in the numeric distribution of values, including outliers, variance, skew and more


## **Average**

Average Validations gauge performance in transitional databases and search engines, offering valuable insights into overall effectiveness.


**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - avg_price:
      on: avg(price)
      where: "country_code = 'IN'"
      threshold: "< 190"
```


## **Minimum**

Minimum Validations ensure consistency across transitional databases and search engines, enhancing data quality and retrieval accuracy.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - min_price:
      on: min(price)
      threshold: "> 0"
```


## **Maximum**

Maximum Validations gauge the highest values within datasets, helping identify outliers and understand data distribution's upper limits for quality assessment.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - max_price:
      on: max(price)
      threshold: "< 1000"
```

## **Sum**

Sum Validations measure the total of all values within a dataset, indicating the overall size of a particular dataset to help understand data quality.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - sum_of_price:
      on: sum(price)
      threshold: "> 100 & < 1000"
```

## **Variance**

Variance in data quality measures the degree of variability or dispersion in a dataset, indicating how spread out the data points are from the mean.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - variance_of_price:
      on: variance(price)
      threshold: "< 2.0"
```

## **Standard Deviation**

Standard deviation Validations measure the amount of variation or dispersion of a set of values from the mean, indicating how spread out the data points are from the mean.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - standard_deviation_of_price:
      on: stddev(price)
      threshold: "< .81"
```

# **Numeric Percentile Validations**

## **20th Percentile**

The 20th Percentile Validation checks the value below which 20% of the data points fall, offering insight into the lower end of the data distribution.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - percentile_20_price:
      on: percentile(price, 20)
      threshold: "> 20"
```
## **40th Percentile**

The 40th Percentile Validation identifies the value below which 40% of the data points fall, providing insight into the data distribution's lower-middle range.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - percentile_40_price:
      on: percentile(price, 40)
      threshold: "> 40"
```
## **60th Percentile**

The 60th Percentile Validation checks the value below which 60% of the data points fall, helping understand the distribution of values around the middle of the dataset.
**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - percentile_60_price:
      on: percentile(price, 60)
      threshold: "> 60"
```
## **80th Percentile**

The 80th Percentile Validation examines the value below which 80% of the data points fall, offering insights into the upper-middle range of the dataset.

**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - percentile_80_price:
      on: percentile(price, 80)
      threshold: "< 500"
```
## **90th Percentile**

The 90th Percentile Validation identifies the value below which 90% of the data points fall, focusing on the upper range of the dataset.

Example
**Example**

```yaml title="dcs_config.yaml"

validations for product_db.products:
  - percentile_90_price:
      on: percentile(price, 90)
      threshold: "< 1000"
```