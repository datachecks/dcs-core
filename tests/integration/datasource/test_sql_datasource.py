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
import datetime

import pytest
from _decimal import Decimal
from loguru import logger
from sqlalchemy import text

from datachecks.core.common.models.configuration import (
    DataSourceConnectionConfiguration,
)
from datachecks.integrations.databases.postgres import PostgresDataSource
from tests.utils import create_postgres_connection

OPEN_SEARCH_DATA_SOURCE_NAME = "test_open_search_data_source"
POSTGRES_DATA_SOURCE_NAME = "test_postgres_data_source"


@pytest.mark.usefixtures("postgres_datasource", "pgsql_connection_configuration")
class TestSQLDataSourceTableColumnMetadata:
    TABEL_NAME = "numeric_metric_test"

    @pytest.fixture(scope="class", autouse=True)
    def setup_tables(
        self, pgsql_connection_configuration: DataSourceConnectionConfiguration
    ):
        postgresql_connection = create_postgres_connection(
            pgsql_connection_configuration
        )
        try:
            postgresql_connection.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.TABEL_NAME} (
                        name VARCHAR(50),
                        age INT,
                        income FLOAT,
                        is_active BOOLEAN,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        location TEXT,
                        extra JSONB,
                        number_of_children SERIAL,
                        short_name CHARACTER(5),
                        house_locations VARCHAR(50)[]
                    )
                """
                )
            )
            postgresql_connection.execute(
                text(
                    f"""
                CREATE TABLE IF NOT EXISTS {self.TABEL_NAME}_2 ( name VARCHAR(50), age INT )
            """
                )
            )
            postgresql_connection.commit()
            yield True
        except Exception as e:
            print(e)
        finally:
            postgresql_connection.execute(
                text(f"DROP TABLE IF EXISTS {self.TABEL_NAME}")
            )
            postgresql_connection.commit()

            postgresql_connection.close()

    def test_table_column_metadata(self, postgres_datasource: PostgresDataSource):
        table_column_metadata = postgres_datasource.query_get_column_metadata(
            table_name=self.TABEL_NAME
        )
        logger.info(table_column_metadata)

        assert table_column_metadata["name"] == "str"
        assert table_column_metadata["age"] == "int"
        assert table_column_metadata["income"] == "float"
        assert table_column_metadata["is_active"] == "bool"
        assert table_column_metadata["created_at"] == "datetime"
        assert table_column_metadata["updated_at"] == "datetime"
        assert table_column_metadata["location"] == "str"
        assert table_column_metadata["extra"] == "dict"
        assert table_column_metadata["number_of_children"] == "int"
        assert table_column_metadata["short_name"] == "str"
        assert table_column_metadata["house_locations"] == "list"

    def test_table_metadata(self, postgres_datasource: PostgresDataSource):
        tables = postgres_datasource.query_get_table_metadata()

        assert self.TABEL_NAME in tables
        assert f"{self.TABEL_NAME}_2" in tables


@pytest.mark.usefixtures("postgres_datasource", "pgsql_connection_configuration")
class TestSQLDatasourceQueries:
    TABLE_NAME = "numeric_metric_test_one"

    @pytest.fixture(scope="class", autouse=True)
    def setup_tables(
        self, pgsql_connection_configuration: DataSourceConnectionConfiguration
    ):
        postgresql_connection = create_postgres_connection(
            pgsql_connection_configuration
        )
        try:
            postgresql_connection.execute(
                text(
                    f"""
                        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                            name VARCHAR(50), last_fight timestamp, age INTEGER,
                            weight FLOAT, description VARCHAR(100), weapon_id VARCHAR(50),
                            usa_phone VARCHAR(50),
                            email VARCHAR(50),
                            usa_state_code VARCHAR(5), usa_zip_code VARCHAR(50),
                            latitude FLOAT,
                            longitude FLOAT,
                            ssn VARCHAR(50),
                            sedol VARCHAR(7),
                            lei VARCHAR(20),
                            cusip VARCHAR(9),
                            figi VARCHAR(12),
                            isin VARCHAR(12)
                        )
                    """
                )
            )

            utc_now = datetime.datetime.utcnow()
            insert_query = f"""
                INSERT INTO {self.TABLE_NAME} VALUES
                ('thor', '{(utc_now - datetime.timedelta(days=10)).strftime("%Y-%m-%d")}',
                    1500, NULL, 'thor hammer', 'e7194aaa-5516-4362-a5ff-6ff971976bec',
                    '123-456-7890', 'jane.doe@domain', 'C2', 'ABCDE', 40.0678, -7555555554.0060,'856-45-6789','0067340',
                    'JRIK0092LOAUCXTR6042','03783310','BBG000B9XRY4','US0378331005'), -- invalid email -- invalid usa_state_code  -- invalid usa_zip_code -- invalid cusip
                ('captain america', '{(utc_now - datetime.timedelta(days=3)).strftime("%Y-%m-%d")}',
                    90, 80, 'shield', 'e7194aaa-5516-4362-a5ff-6ff971976b', '(123) 456-7890',
                    'john.doe@.com ', 'NY', '12-345', 34.0522, -118.2437,'000-12-3456', 'B01HL06',
                    'CDR300OS7OJENVEDDW89','037833100','BBG000BL2H25','US5949181045'), -- invalid weapon_id --invalid email -- invalid usa_zip_code -- invalid ssn
                ('iron man', '{(utc_now - datetime.timedelta(days=4)).strftime("%Y-%m-%d")}',
                    50, 70, 'suit', '1739c676-6108-4dd2-8984-2459df744936', '123 456 7890',
                    'contact@company..org', 'XY', '85001', 37.7749, -122.4194,'859-99-9999','4155586',
                    'VXQ400F1OBWAVPBJP86','594918104','BBG000B3YB97','US38259P5088'), -- invalid email -- invalid usa_state_code -- invalid lei
                ('hawk eye', '{(utc_now - datetime.timedelta(days=5)).strftime("%Y-%m-%d")}',
                    40, 60, 'bow', '1739c676-6108-4dd2-8984-2459df746', '+1 123-456-7890',
                    'user@@example.com', 'TX', '30301', 51.1657, 10.4515,'123-45-67890','12345',
                    'FKRD00GCEYWDCNYLNF60','38259P508','BBG000B57Y12','US83165F1026'), -- invalid weapon_id --invalid email -- invalid ssn -- invalid sedol
                ('clark kent', '{(utc_now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")}',
                    35, 50, '', '7be61b2c-45dc-4889-97e3-9202e8', '09123.456.7890',
                    'contact@company.org', 'ZZ', '123456', 51.5074, -0.1278,'666-45-6789','34A56B7',
                    '6R5J00FMIANQQH6JMN56','83165F102','BBG000B9XRY','US0231351067'), -- invalid weapon_id -- invalid phone -- invalid usa_state_code -- invalid usa_zip_code -- invalid ssn -- invalid sedol -- invalid figi
                ('black widow', '{(utc_now - datetime.timedelta(days=6)).strftime("%Y-%m-%d")}',
                    35, 50, '', '7be61b2c-45dc-4889-97e3-9202e8032c73', '+1 (123) 456-7890',
                    'jane_smith123@domain.co.uk', 'FL', '90210', 483.8566, 2.3522,'001-01-0001','456VGHY',
                    '0FPB00BBRHUYOE7DSK19','023135106','BBG000B6R530','US037833100') -- invalid isin -- invalid sedol
            """

            postgresql_connection.execute(text(insert_query))

            postgresql_connection.commit()
            yield True
        except Exception as e:
            print(e)
        finally:
            postgresql_connection.execute(
                text(f"DROP TABLE IF EXISTS {self.TABLE_NAME}")
            )
            postgresql_connection.commit()

            postgresql_connection.close()

    def test_should_return_numeric_profile(
        self, postgres_datasource: PostgresDataSource
    ):
        profile = postgres_datasource.profiling_sql_aggregates_numeric(
            self.TABLE_NAME, "age"
        )
        assert profile["min"] == 35
        assert profile["max"] == 1500
        assert profile["sum"] == 1750
        assert round(profile["avg"], 2) == Decimal("291.67")

    def test_should_return_text_profile(self, postgres_datasource: PostgresDataSource):
        profile = postgres_datasource.profiling_sql_aggregates_string(
            self.TABLE_NAME, "name"
        )
        assert profile["distinct_count"] == 6
        assert profile["missing_count"] == 0
        assert profile["max_length"] == 15

    def test_should_return_avg_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_avg(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 767.5

    def test_should_return_min_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_min(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 35

    def test_should_return_max_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_max(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1500

    def test_should_return_sum_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_sum(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1535

    def test_should_return_variance_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_variance(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1073112.5

    def test_should_return_stddev_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_stddev(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == round(Decimal(1035.91), 2)

    def test_should_return_duplicate_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_duplicate_count(
            table=self.TABLE_NAME,
            field="age",
        )
        assert result == 1

    def test_should_return_null_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_null_count(
            table=self.TABLE_NAME,
            field="weight",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1

    def test_should_return_empty_string_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_empty_string_count(
            table=self.TABLE_NAME,
            field="description",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 1

    def test_should_return_empty_string_percentage_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_empty_string_percentage(
            table=self.TABLE_NAME,
            field="description",
            filters="name in ('thor', 'black widow')",
        )
        assert round(result, 2) == 50.0

    def test_should_return_distinct_count_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_distinct_count(
            table=self.TABLE_NAME,
            field="age",
            filters="name in ('thor', 'black widow')",
        )
        assert result == 2

    def test_should_return_null_percentage_with_filter(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_null_percentage(
            table=self.TABLE_NAME,
            field="weight",
            filters="name in ('thor', 'black widow')",
        )
        assert round(result, 2) == 50.0

    def test_should_return_time_diff_in_second(
        self, postgres_datasource: PostgresDataSource
    ):
        time_diff = postgres_datasource.query_get_time_diff(
            self.TABLE_NAME, field="last_fight"
        )
        assert time_diff >= 3 * 24 * 3600

    def test_should_return_row_count(self, postgres_datasource: PostgresDataSource):
        row_count = postgres_datasource.query_get_row_count(self.TABLE_NAME)
        assert row_count == 6

    def test_should_run_custom_sql(self, postgres_datasource: PostgresDataSource):
        row_count = postgres_datasource.query_get_custom_sql(
            query=f"select count(*), avg(age) from {self.TABLE_NAME}"
        )
        assert row_count == 6

    def test_should_run_valid_uuid(self, postgres_datasource: PostgresDataSource):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="weapon_id", predefined_regex_pattern="uuid"
        )
        assert valid_count == 3
        assert total_row_count == 6

    def test_should_run_count_invalid_values(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            invalid_count,
            total_row_count,
        ) = postgres_datasource.query_valid_invalid_values_validity(
            table=self.TABLE_NAME, field="name", values=["thor"]
        )
        assert invalid_count == 1
        assert total_row_count == 6

    def test_should_run_count_invalid_regex(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            invalid_count,
            total_row_count,
        ) = postgres_datasource.query_valid_invalid_values_validity(
            table=self.TABLE_NAME, field="name", regex_pattern=".la.*"
        )
        assert invalid_count == 2

    def test_should_return_row_count_for_valid_usa_phone_number(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME,
            field="usa_phone",
            predefined_regex_pattern="usa_phone",
        )
        assert valid_count == 5
        assert total_row_count == 6

    def test_should_return_row_count_for_valid_email(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="email", predefined_regex_pattern="email"
        )
        assert valid_count == 2
        assert total_row_count == 6

    def test_should_return_string_length_max(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_string_length_metric(
            table=self.TABLE_NAME,
            field="name",
            filters="name in ('thor', 'black widow')",
            metric="max",
        )
        assert result == 11

    def test_should_return_string_length_min(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_string_length_metric(
            table=self.TABLE_NAME,
            field="name",
            filters="name in ('black widow', 'iron man')",
            metric="min",
        )
        assert result == 8

    def test_should_return_string_length_avg(
        self, postgres_datasource: PostgresDataSource
    ):
        result = postgres_datasource.query_get_string_length_metric(
            table=self.TABLE_NAME,
            field="name",
            filters="name in ('thor', 'black widow')",
            metric="avg",
        )
        assert result == 7.5

    def test_should_return_row_count_for_valid_usa_zip_code(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME,
            field="usa_zip_code",
            predefined_regex_pattern="usa_zip_code",
        )
        assert valid_count == 3
        assert total_row_count == 6

    def test_should_return_row_count_for_valid_usa_state_code(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_get_usa_state_code_validity(
            table=self.TABLE_NAME,
            field="usa_state_code",
        )
        assert valid_count == 3
        assert total_row_count == 6

    def test_should_return_count_latitude(
        self, postgres_datasource: PostgresDataSource
    ):
        count_latitude = postgres_datasource.query_geolocation_metric(
            table=self.TABLE_NAME, field="latitude", operation="count"
        )
        assert count_latitude == 5

    def test_should_return_percent_latitude(
        self, postgres_datasource: PostgresDataSource
    ):
        percent_latitude = postgres_datasource.query_geolocation_metric(
            table=self.TABLE_NAME, field="latitude", operation="percent"
        )
        assert round(percent_latitude, 2) == 83.33

    def test_should_return_count_longitude(
        self, postgres_datasource: PostgresDataSource
    ):
        count_longitude = postgres_datasource.query_geolocation_metric(
            table=self.TABLE_NAME, field="longitude", operation="count"
        )
        assert count_longitude == 5

    def test_should_return_percent_longitude(
        self, postgres_datasource: PostgresDataSource
    ):
        percent_longitude = postgres_datasource.query_geolocation_metric(
            table=self.TABLE_NAME, field="longitude", operation="percent"
        )
        assert round(percent_longitude, 2) == 83.33

    def test_should_return_row_count_for_valid_ssn(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="ssn", predefined_regex_pattern="ssn"
        )
        assert valid_count == 3
        assert total_row_count == 6

    def test_should_return_count_for_valid_sedol(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="sedol", predefined_regex_pattern="sedol"
        )
        assert valid_count == 3
        assert total_row_count == 6

    def test_should_return_count_for_valid_cusip(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="cusip", predefined_regex_pattern="cusip"
        )
        assert valid_count == 5
        assert total_row_count == 6

    def test_should_return_count_for_valid_lei(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="lei", predefined_regex_pattern="lei"
        )
        assert valid_count == 5
        assert total_row_count == 6

    def test_should_return_count_for_valid_figi(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="figi", predefined_regex_pattern="figi"
        )
        assert valid_count == 5
        assert total_row_count == 6

    def test_should_return_count_for_valid_isin(
        self, postgres_datasource: PostgresDataSource
    ):
        (
            valid_count,
            total_row_count,
        ) = postgres_datasource.query_string_pattern_validity(
            table=self.TABLE_NAME, field="isin", predefined_regex_pattern="isin"
        )
        assert valid_count == 5
        assert total_row_count == 6
