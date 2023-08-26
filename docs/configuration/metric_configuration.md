# **Metric Configuration**

Datachecks will read metrics configuration under the key `metrics` in the configuration file. User can define multiple metrics in the configuration file under `metrics` key.

For example:

```yaml
metrics:
  - name: freshness
    type: freshness
    resource: mysql_db.table_name.last_updated
```

## Configuration Details

| Parameter  | Mandatory        | Description                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:-----------|:-----------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`     | :material-check: | The name of the metric. The name should be unique.                                                                                                                                                                                                                                                                                                                                                                           |
| `type`     | :material-check: | The type of the metric. Possible values are `freshness`, `row_count` etc. Type of metric mentioned in every metric documentation                                                                                                                                                                                                                                                                                             |
| `resource` | :material-check: | The resource for which metric should be generates. A resource can be a Table, Index, Collection or Field. </br></br> In case of Table, Index or Collection the pattern of the resource name would be `<datasource>.<table_name>` or `<datasource>.<index_name>` </br></br> In case of a Field the pattern of the resource name would be `<datasource>.<table_name>.<field_name>` or `<datasource>.<index_name>.<field_name>` |
| `filters`  | :material-close: | The filters to be applied on the resource. Filters can have `where` as a nested field.</br>In `where` field we can pass `SQL Query`(In ase of SQl DB) or `Search Query`(In ase of search engine). </br></br>For example: </br> `filters:`</br>&emsp;`where: city = 'bangalore' AND age >= 30`                                                                                                                                |
