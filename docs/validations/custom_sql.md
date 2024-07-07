# **Custom SQL Validation**

If the built-in set of validations does not quite give you the information you need from a Validation, you have the flexibility to define your own Validations using `custom_sql`.

The custom SQL Validation empowers you to enter your own completely custom SQL query, providing you with the ability to create much more complex and specify monitors according to your specific requirements. This feature allows you to dig deeper into your data and extract insights that are tailored to your unique needs.

## Example

```yaml
validations for mysql_db.student:
  - custom_sql_example:
      on: custom_sql
      query: |
        SELECT COUNT(*) FROM student WHERE city = 'bangalore' AND age >= 30
```