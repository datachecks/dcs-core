# **Custom SQL Metrics**

If the built-in set of metrics does not quite give you the information you need from a metric, you have the flexibility to define your own metrics using `custom_sql`.

The custom SQL metric empowers you to enter your own completely custom SQL query, providing you with the ability to create much more complex and specify monitors according to your specific requirements. This feature allows you to dig deeper into your data and extract insights that are tailored to your unique needs.

## Example

```yaml
metrics:
  - name: custom_sql_example
    type: custom_sql
    resource: mysql_db.student
    query: |
      SELECT COUNT(*) FROM student WHERE city = 'bangalore' AND age >= 30
```