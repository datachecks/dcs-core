# **Validity Validations**

Validity checks ensure data is not only correctly formatted but also valid. These metrics are crucial for data quality assurance by verifying adherence to predefined rules and standards. Implementing these checks helps users detect and fix errors, maintaining data integrity and reliability.

## Count UUID

The count UUID validation checks the number of UUIDs in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count uuid for product_id:
      on: count_uuid(product_id)
      threshold: "> 100"
```

## Percentage UUID

The percentage UUID validation checks the percentage of UUIDs in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percentage uuid for product_id:
      on: percent_uuid(product_id)
      threshold: "> 90"
```

##  Count Invalid Values

The count invalid values validation checks how many entries in a dataset are invalid according to given values.

**Example**
```yaml title="dcs_config.yaml"
  - invalid values count for species:
      on: count_invalid_values(species)
      values: ["versicolor"]
```

## Percent Invalid Values
The percent invalid values validation checks the percentage of entries in a dataset that are invalid according to given values.

**Example**

```yaml title="dcs_config.yaml"
validations for iris_db.iris:
  - invalid values percentage for species:
      on: percent_invalid_values(species)
      values: ["versicolor"]
```

## Count Valid Values

The count valid values validation checks how many entries in a dataset are valid according to given values.

**Example**

```yaml title="dcs_config.yaml"
  - valid values count for species:
      on: count_valid_values(species)
      values: ["setosa", "virginica"]
```

## Percent Valid Values

The percent valid values validation checks the percentage of entries in a dataset that are valid according to given values.

**Example**

```yaml title="dcs_config.yaml"
validations for iris_db.iris:
  - valid values percentage for species:
      on: percent_valid_values(species)
      values: ["setosa", "virginica"]
      threshold: "> 65"
```

## Count Invalid Regex

The count invalid regex validation checks how many entries in a dataset are invalid according to a given regex pattern.

**Example**

```yaml title="dcs_config.yaml"
  - invalid regex count for species:
      on: count_invalid_regex(species)
      pattern: "^(setosa|virginica)$"
```

## Percent Invalid Regex

The percent invalid regex validation checks the percentage of entries in a dataset that are invalid according to a given regex pattern.

**Example**

```yaml title="dcs_config.yaml"
validations for iris_db.iris:
  - invalid regex percentage for species:
      on: percent_invalid_regex(species)
      pattern: "^(setosa|virginica)$"
      threshold: "> 10"
```

## Count Valid Regex

The count valid regex validation checks how many entries in a dataset are valid according to a given regex pattern.

**Example**

```yaml title="dcs_config.yaml"
  - valid regex count for species:
      on: count_valid_regex(species)
      pattern: "^(setosa|virginica)$"
```

## Percent Valid Regex

The percent valid regex validation checks the percentage of entries in a dataset that are valid according to a given regex pattern.

**Example**

```yaml title="dcs_config.yaml"
validations for iris_db.iris:
  - valid regex percentage for species:
      on: percent_valid_regex(species)
      pattern: "^(setosa|virginica)$"
      threshold: "> 90"
```


## Count USA Phone Number

The count USA phone number validation checks the number of valid USA phone numbers in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for customer_db.customers:
  - count USA phone number for phone_number:
      on: on: count_usa_phone(usa_phone_number)
      threshold: "> 100"
```

## Percentage USA Phone Number

The percentage USA phone number validation checks the percentage of valid USA phone numbers in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for customer_db.customers:
  - percentage USA phone number for phone_number:
      on: percent_usa_phone(usa_phone_number)
      threshold: "> 90"
```