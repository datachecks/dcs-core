#  Copyright 2022-present, the Waterdip Labs Pvt. Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os

import findspark
import pandas as pd
from pyspark.sql import SparkSession

from datachecks.core import Inspect

os.environ["SPARK_HOME"] = "/opt/homebrew/Cellar/apache-spark/3.5.1/libexec/"

findspark.init()
spark = SparkSession.builder.getOrCreate()

data = pd.DataFrame(
    {
        "State": ["Alaska", "California", "Florida", "Washington"],
        "city": ["Anchorage", "Los Angeles", "Miami", "Bellevue"],
        "age": [1, 2, 3, 4],
    }
)

# create DataFrame
df_spark = spark.createDataFrame(data)
df_spark.createOrReplaceTempView("employees")

inspect = Inspect()

config = """
validations for dcs.employees:
  - check row count:
      on: min(age)
      threshold: "=1"
  - check count rows:
      on: count_rows
      threshold: "< 1"
"""

inspect.add_spark_session(spark, data_source_name="dcs")
inspect.add_validations_yaml_str(config)

inspect_result = inspect.run()
print(inspect_result)
