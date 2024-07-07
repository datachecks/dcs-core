# **Combined Metrics**

Combined metrics in data quality serve as a cornerstone for ensuring the accuracy and efficiency of your data operations. These metrics provide a holistic view of your data ecosystem, amalgamating various aspects to paint a comprehensive picture.

By consistently tracking these combined metrics, you gain invaluable insights into the overall performance of your data infrastructure. This data-driven approach enables you to make informed decisions on optimization, resource allocation, and system enhancements. Moreover, these metrics act as sentinels, promptly detecting anomalies or bottlenecks within your data pipelines. This proactive stance allows you to mitigate potential issues before they escalate, safeguarding the integrity of your data.


## **Available Function**

- `div()`
- `sum()`
- `mul()`
- `sub()`

### Single Function Expression
**Example**

```yaml title="dcs_config.yaml"
metrics:
- name: combined_metric_example
  metric_type: combined
  expression: sum(count_product_invalid, count_product_valid)
```
### Multiple Functions Expression

We can combine multiple functions in one expression.

**Example**

```yaml title="dcs_config.yaml"
metrics:
- name: combined_metric_example
  metric_type: combined
  expression: div(sum(count_product_invalid, count_product_valid), count_product)
```
### Multiple Functions Expression with numeric values

We can combine multiple functions in one expression with numeric values.

**Example**

```yaml title="dcs_config.yaml"
metrics:
- name: combined_metric_example
  metric_type: combined
  expression: div(sum(count_product_invalid, 10), count_product)
```

## Limitations
Combined metrics accepts only 2 arguments in one functions.
