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
validations for iris_db.iris:
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
validations for iris_db.iris:
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
validations for iris_db.iris:
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
validations for iris_db.iris:
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

## Count Email

The count email validation checks the number of valid email addresses in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for customer_db.customers:
  - count email for email:
      on: count_email(email)
```

## Percentage Email

The percentage email validation checks the percentage of valid email addresses in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for customer_db.customers:
  - percentage email for email:
      on: percent_email(email)
      threshold: "> 90"
```
## String Length Max

The StringLengthMaxValidation checks the maximum length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name max length:
      on: string_length_max(product_name)
      threshold: "<= 100"
```

## String Length Min

The StringLengthMinValidation checks the minimum length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name min length:
      on: string_length_min(product_name)
      threshold: ">= 5"
```

## String Length Average

The StringLengthAverageValidation checks the average length of strings in a specified column.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - product name average length:
      on: string_length_average(product_name)
      threshold: ">= 10"
```


# Geolocation Validations

## Count Latitude

The `CountLatitudeValidation` checks the number of non-null and valid latitude values (ranging between -90 and 90) in a specified column.

**Example**

```yaml
validations for location_db.geolocation:
  - location latitude count:
      on: count_latitude(latitude_column_name)
      threshold: "> 100"
```

## Percent Latitude

The `PercentLatitudeValidation` checks the percentage of non-null and valid latitude values (ranging between -90 and 90) in a specified column.

**Example**

```yaml
validations for location_db.geolocation:
  - location latitude percentage:
      on: percent_latitude(latitude_column_name)
      threshold: "> 80"
```

## Count Longitude

The `CountLongitudeValidation` checks the number of non-null and valid longitude values (ranging between -180 and 180) in a specified column.

**Example**

```yaml
validations for location_db.geolocation:
  - location longitude count:
      on: count_longitude(longitude_column_name)
      threshold: "> 100"
```

## Percent Longitude

The `PercentLongitudeValidation` checks the percentage of non-null and valid longitude values (ranging between -180 and 180) in a specified column.

**Example**

```yaml
validations for location_db.geolocation:
  - location longitude percentage:
      on: percent_longitude(longitude_column_name)
      threshold: "> 80"
```


## Count SSN

The count ssn validation checks the number of valid ssn(social security number) in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count ssn of users:
     on: count_ssn(ssn_number)
```

## Percent SSN

The percent ssn validation checks the percentage of valid ssn(social security number) in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_ssn_of_user:
      on: percent_ssn(ssn_number)
```

## Count SEDOL

The count sedol validation checks the number of valid sedol in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count sedol of users:
     on: count_sedol(sedol_number)
```

## Percent SEDOL

The percent sedol validation checks the percentage of valid sedol in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_sedol_of_user:
      on: percent_sedol(sedol_number)
```

## Count CUSIP

The count cusip validation checks the number of valid cusip in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count cusip of users:
     on: count_cusip(cusip_number)
```

## Percent CUSIP

The percent cusip validation checks the percentage of valid cusip in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_cusip_of_user:
      on: percent_cusip(cusip_number)
```

## Count LEI

The count lei validation checks the number of valid lei in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count lei of users:
     on: count_lei(lei_number)
```

## Percent LEI

The percent lei validation checks the percentage of valid lei in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_lei_of_user:
      on: percent_lei(lei_number)
```

## Count FIGI

The count figi validation checks the number of valid figi in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count figi of users:
     on: count_figi(figi_number)
```

## Percent FIGI

The percent figi validation checks the percentage of valid figi in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_figi_of_user:
      on: percent_figi(figi_number)
```

## Count ISIN

The count isin validation checks the number of valid isin in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count isin of users:
     on: count_isin(isin_number)
```

## Percent ISIN

The percent isin validation checks the percentage of valid isin in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_isin_of_user:
      on: percent_isin(isin_number)
```

## Count PermID

The count permid validation checks the number of valid permid in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count permid of users:
     on: count_permid(perm_id)
```

## Percent PermID

The percent permid validation checks the percentage of valid permid in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_permid_of_user:
      on: percent_permid(perm_id)
```

## Zero Value Validations

Zero value validations are used to identify and validate fields that contain zero values, which are important for datasets where zero values might have specific implications, such as indicating missing or invalid data, or representing real-world conditions.

## `COUNT_ZERO`

`COUNT_ZERO` is used to count the number of rows where the specified field contains a zero value. It can be useful for detecting cases where zero might represent missing data or special conditions.

**Example:**

```yaml
validations for product_db.products:
  - price zero count:
      on: count_zero(price)
      threshold: "< 52"
```

## `PERCENT_ZERO`

`PERCENT_ZERO` is used to calculate the percentage of rows where the specified field contains a zero value. This helps assess the proportion of zero values in a column, allowing the user to enforce percentage-based thresholds for data quality.

**Example:**

```yaml
validations for product_db.products:
  - price zero percent:
      on: percent_zero(price)
      threshold: "< 10"

# **Numeric Negative Value Validations**

The Numeric Negative Value Validations detect negative values in numeric fields within a dataset and ensure that they do not exceed or fall below a specified threshold.

## **COUNT_NEGATIVE**

This validation counts the number of negative values present in a given numeric field.

**Example**

```yaml
validations for product_db.products:
  - negative value count should be less than 2:
      on: count_negative(price)
      threshold: "< 2"
```

## **PERCENT_NEGATIVE**

This validation calculates the percentage of negative values in a numeric field, relative to the total number of records.

**Example**

```yaml
validations for product_db.products:
  - negative value percentage should be less than 40%:
      on: percent_negative(price)
      threshold: "< 40"
```
## COUNT_ALL_SPACE

The count all space validation counts columns with all space values in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count_all_space_value:
      on: count_all_space(space)
      threshold: = 0
```

## PERCENT_ALL_SPACE

The percent all space validation checks the percentage of columns with all space value in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_all_space:
      on: percent_all_space(space)
```

## COUNT_NULL_KEYWORD

The count null keyword validation counts the number of null like keyword in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count_null_keyword:
     on: count_null_keyword(keyword)
     threshold: <=10
```

## PERCENT_NULL_KEYWORD

The percent null keyword validation checks the percentage of null like keyword in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_null_keyword:
     on: percent_null_keyword(keyword)
```

## Count Timestamp String

The count timestamp string validation checks the number of valid timestamp string in ISO format in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count_valid_timestamp:
     on: count_timestamp_string(timestamp)
```

## Percent Timestamp String

The percent timestamp string validation checks the percentage of valid timestamp string in ISO format in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_valid_timestamp:
     on: percent_timestamp_string(timestamp)
```

## Count Not In Future

The count not in future validation checks the number of valid timestamp string that are not in future in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count_timestamp_not_in_future:
     on: count_not_in_future(future_timestamp)
```

## Percent Not In Future

The percent date not in future validation checks the percentage of valid timestamp string that are not in future in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_timestamp_not_in_future:
     on: percent_not_in_future(future_timestamp)
```

## Count Date Not In Future

The count date not in future validation checks the number of valid timestamp string with date that are not in future in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - count_date_not_in_future:
     on: count_date_not_in_future(future_timestamp)
```

## Percent Date Not In Future

The percent date not in future validation checks the percentage of valid timestamp string with date that are not in future in a dataset.

**Example**

```yaml title="dcs_config.yaml"
validations for product_db.products:
  - percent_date_not_in_future:
     on: percent_date_not_in_future(future_timestamp)
```