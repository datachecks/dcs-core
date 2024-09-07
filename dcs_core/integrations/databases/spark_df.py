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
from typing import Any, List, Union

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row

from dcs_core.core.datasource.sql_datasource import SQLDataSource


class SparkDfCursor:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.df: Union[DataFrame, None] = None
        self.description: Union[tuple[tuple], None] = None
        self.rowcount: int = -1
        self.cursor_index: int = -1

    def execute(self, sql: str):
        self.df = self.spark_session.sql(sqlQuery=sql)
        self.description = self.convert_spark_df_schema_to_dbapi_description(self.df)
        self.cursor_index = 0

    def fetchall(self) -> tuple[List, ...]:
        rows = []
        spark_rows: list[Row] = self.df.collect()
        self.rowcount = len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchmany(self, size: int) -> tuple[List, ...]:
        rows = []
        self.rowcount = self.df.count()
        spark_rows: list[Row] = self.df.limit(size).offset(self.cursor_index).collect()
        self.cursor_index += len(spark_rows)
        for spark_row in spark_rows:
            row = self.convert_spark_row_to_dbapi_row(spark_row)
            rows.append(row)
        return tuple(rows)

    def fetchone(self) -> tuple:
        spark_rows: list[Row] = self.df.collect()
        self.rowcount = len(spark_rows)
        spark_row = spark_rows[0]
        row = self.convert_spark_row_to_dbapi_row(spark_row)
        return tuple(row)

    @staticmethod
    def convert_spark_row_to_dbapi_row(spark_row):
        return [spark_row[field] for field in spark_row.__fields__]

    def close(self):
        pass

    @staticmethod
    def convert_spark_df_schema_to_dbapi_description(df) -> tuple[tuple[Any, Any], ...]:
        return tuple(
            (field.name, type(field.dataType).__name__) for field in df.schema.fields
        )


class SparkDfConnection:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def cursor(self) -> SparkDfCursor:
        return SparkDfCursor(self.spark_session)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


class SparkDFDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: dict):
        super().__init__(data_source_name, data_connection)
        self.spark_session = data_connection.get("spark_session")
        self.use_sa_text_query = False

    def connect(self):
        self.connection = SparkDfConnection(self.spark_session)

    def close(self):
        pass

    def fetchone(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()

    def fetchall(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
