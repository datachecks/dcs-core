# **Numeric Distribution Metrics**

Numeric Distribution metrics detect changes in the numeric distribution of values, including outliers, variance, skew and more


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
**Coming Soon..**

## **Kurtosis**
**Coming Soon..**

## **Geometric Mean**
**Coming Soon..**

## **Harmonic Mean**
**Coming Soon..**