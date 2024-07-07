# **Metric Configuration**

Datachecks will read metrics configuration under the key `metrics` in the configuration file. User can define multiple metrics in the configuration file under `metrics` key.

For example:

```yaml
validations for mysql_db.table_name:
  - freshness_example:
      on: freshness(last_updated)
      threshold: "> 86400" ##Freshness metric value is in seconds. Validation error if metric value is greater than 86400 seconds.
```

## Configuration Details

| Parameter     | Mandatory        | Description                                                                                                                                                                                                                                                                                           |
|:--------------|:-----------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<key filed>` | :material-check: | The name of the validation. The name should be unique.                                                                                                                                                                                                                                                |
| `on`          | :material-check: | The type of the validation function. Possible values are `freshness`, `row_count` etc. Type of validation mentioned in every metric documentation                                                                                                                                                     |
| `where`       | :material-close: | The where filter to be applied on the filed. In `where` field we can pass `SQL Query`(In ase of SQl DB) or `Search Query`(In ase of search engine). </br></br>For example: </br> `where: city = 'bangalore' AND age >= 30`                                                                            |
| `threshold`   | :material-close: | The validation will be applied on the validation value. A validation error will be invoked if the metric value violate threshold value. </br> Possible values for threshold are `>`, `>=`, `=` , `<`, `<=`. We can combine multiple operators  </br> For example: </br> `threshold: ">= 10 & <= 100"` |


## Validation Types

Supported Validation functions are


| Validation Group         | Validation Type                                                                                       |
|:---------------------|:--------------------------------------------------------------------------------------------------|
| Reliability          | [Freshness](https://docs.datachecks.io/metrics/reliability/#freshness)                            |
| Reliability          | [Row Count](https://docs.datachecks.io/metrics/reliability/#row-count)                            |
| Reliability          | [Document Count](https://docs.datachecks.io/metrics/reliability/#document-count)                  |
| Numeric Distribution | [Average](https://docs.datachecks.io//metrics/numeric_distribution/#average)                      |
| Numeric Distribution | [Minimum](https://docs.datachecks.io/metrics/numeric_distribution/#minimum)                       |
| Numeric Distribution | [Maximum](https://docs.datachecks.io/metrics/numeric_distribution/#maximum)                       |
| Numeric Distribution | [Sum](https://docs.datachecks.io/metrics/numeric_distribution/#sum)                               |
| Numeric Distribution | [Variance](https://docs.datachecks.io/metrics/numeric_distribution/#variance)                     |
| Numeric Distribution | [Standard Deviation](https://docs.datachecks.io/metrics/numeric_distribution/#standard-deviation) |
| Uniqueness           | [Distinct Count](https://docs.datachecks.io/metrics/uniqueness/#distinct-count)                   |
| Uniqueness           | [Duplicate Count](https://docs.datachecks.io/metrics/uniqueness/#duplicate-count)                 |
| Completeness         | [Null Count](https://docs.datachecks.io/metrics/completeness/#null-count)                         |
| Completeness         | [Null Percentage](https://docs.datachecks.io/metrics/completeness/#null-percentage)               |
| Completeness         | [Empty Count](https://docs.datachecks.io/metrics/completeness/#empty-count)                       |
| Completeness         | [Empty Percentage](https://docs.datachecks.io/metrics/completeness/#empty-percentage)             |
| Special              | [Combined](https://docs.datachecks.io/metrics/combined/)                                          |
| Special              | [Custom SQL](https://docs.datachecks.io/metrics/custom_sql/)                                      |
