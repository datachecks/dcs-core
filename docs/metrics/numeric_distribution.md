# **Numeric Distribution Metrics**

Numeric distribution metrics serve as vital tools for ensuring the ongoing integrity of your data. These metrics offer valuable insights into the distribution of values within your datasets, aiding in data quality assurance.

By consistently monitoring these metrics, you gain a deeper understanding of how your data behaves. This knowledge empowers you to make informed decisions regarding data cleansing, anomaly detection, and overall data quality improvement.

Furthermore, numeric distribution metrics are your early warning system. They help pinpoint outliers and anomalies, allowing you to address potential data issues before they escalate into significant problems in your data pipelines.


## **Average**

Average metrics gauge performance in transitional databases and search engines, offering valuable insights into overall effectiveness.


**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: avg_price
      metric_type: avg
      resource: product_db.products
      field_name: price
      filters:
        where: "country_code = 'IN'"
```


## **Minimum**

Minimum metrics ensure consistency across transitional databases and search engines, enhancing data quality and retrieval accuracy.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: min_price
      metric_type: min
      resource: product_db.products
      field_name: price
```

## **Maximum**

Maximum metrics gauge the highest values within datasets, helping identify outliers and understand data distribution's upper limits for quality assessment.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: max_price
      metric_type: max
      resource: product_db.products
      field_name: price
```

```yaml title="dcs_config.yaml"
- name: max_price_of_products_with_high_rating
      metric_type: max
      resource: product_db.products
      field_name: price
      filters:
        where: "rating > 4"
```

## **Variance**

Variance in data quality measures the degree of variability or dispersion in a dataset, indicating how spread out the data points are from the mean.

**Example**

```yaml title="dcs_config.yaml"
metrics:
    - name: variance_of_price
      metric_type: variance
      resource: product_db.products
      field_name: price
```

## **Skew**

Skew metric in data quality measures the extent of asymmetry or distortion in the distribution of data values. It helps assess the balance and uniformity of data distribution.

**Example**

```yaml title="dcs_config.yaml"

```

## **Kurtosis**

Kurtosis is a data quality metric that measures the level of peakedness or flatness of a dataset's probability distribution in a geometric space.

**Example**

```yaml title="dcs_config.yaml"

```

## **Sum**

The sum metric in data quality measures the accuracy and consistency of numerical data by assessing the total of a specific attribute across different records.

**Example**

```yaml title="dcs_config.yaml"

```

## **Geometric Mean**

The geometric mean metric in data quality is a statistical measure that calculates the nth root of the product of n data values, often used to assess the central tendency of a dataset

**Example**

```yaml title="dcs_config.yaml"

```

## **Harmonic Mean**

The Harmonic mean metric in data quality is a statistical measure used to assess the quality of data by calculating the reciprocal of the average of the reciprocals of data values.

**Example**

```yaml title="dcs_config.yaml"

```