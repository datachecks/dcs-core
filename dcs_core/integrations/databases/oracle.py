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

from typing import Any, Dict, List, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class OracleDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)

        self.regex_patterns = {
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            "usa_phone": r"^(\+1[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$",
            "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
            "usa_zip_code": r"^[0-9]{5}(?:-[0-9]{4})?$",
            "ssn": r"^(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}$",
            "sedol": r"[B-Db-dF-Hf-hJ-Nj-nP-Tp-tV-Xv-xYyZz\d]{6}\d",
            "lei": r"^[A-Z0-9]{18}[0-9]{2}$",
            "cusip": r"^[0-9A-Z]{9}$",
            "figi": r"^BBG[A-Z0-9]{9}$",
            "isin": r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$",
            "perm_id": r"^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{3}$",
        }

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        try:
            engine = create_engine(
                f"oracle+oracledb://:@",
                thick_mode=False,
                connect_args={
                    "user": self.data_connection.get("username"),
                    "password": self.data_connection.get("password"),
                    "host": self.data_connection.get("host"),
                    "port": self.data_connection.get("port"),
                    "service_name": self.data_connection.get("service_name"),
                },
            )
            self.connection = engine.connect()
            return self.connection
        except Exception as e:
            raise DataChecksDataSourcesConnectionError(
                message=f"Failed to connect to Oracle data source: [{str(e)}]"
            )
        

    def query_valid_invalid_values_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        filters: str = None,
        values: List[str] = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid and invalid values
        :param table: table name
        :param field: column name
        :param values: list of valid values
        :param regex_pattern: regex pattern
        :param filters: filter condition
        :return: count of valid/invalid values and total count of valid/invalid values
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)
        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            regex_query = f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
        else:
            regex_query = f"CASE WHEN REGEXP_LIKE({field}, '{regex_pattern}') THEN 1 ELSE 0 END"
        
        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters}
        """
        
        result = self.fetchone(query)
        return result[0], result[1]

    def query_string_pattern_validity(
        self,
        table: str,
        field: str,
        regex_pattern: str = None,
        predefined_regex_pattern: str = None,
        filters: str = None,
    ) -> Tuple[int, int]:
        """
        Get the count of valid values based on the regex pattern
        :param table: table name
        :param field: column name
        :param regex_pattern: regex pattern
        :param predefined_regex_pattern: predefined regex pattern
        :param filters: filter condition
        :return: count of valid values, count of total row count
        """
        filters = f"WHERE {filters}" if filters else ""
        qualified_table_name = self.qualified_table_name(table)

        if not regex_pattern and not predefined_regex_pattern:
            raise ValueError(
                "Either regex_pattern or predefined_regex_pattern should be provided"
            )

        if predefined_regex_pattern:
            regex_condition = f"REGEXP_LIKE({field}, '{self.regex_patterns[predefined_regex_pattern]}')"
        else:
            regex_condition = f"REGEXP_LIKE({field}, '{regex_pattern}')"

        regex_query = f"CASE WHEN {regex_condition} THEN 1 ELSE 0 END"

        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]
    

    def query_get_usa_state_code_validity(
        self, table: str, field: str, filters: str = None
    ) -> Tuple[int, int]:
        """
        Get the count of valid USA state codes
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of valid state codes, count of total row count
        """
        
        valid_state_codes_str = ", ".join(
            f"'{code}'" for code in self.valid_state_codes
        )
        
        filters = f"WHERE {filters}" if filters else ""
        
        qualified_table_name = self.qualified_table_name(table)
        
        regex_query = (
            f"CASE WHEN REGEXP_LIKE({field}, '^[A-Z]{{2}}$') "
            f"AND {field} IN ({valid_state_codes_str}) THEN 1 ELSE 0 END"
        )
        
        query = f"""
            SELECT SUM({regex_query}) AS valid_count, COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """
        
        result = self.fetchone(query)
        return result[0], result[1]

