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

import random
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import pyodbc
from loguru import logger

from dcs_core.core.common.errors import DataChecksDataSourcesConnectionError
from dcs_core.core.common.models.data_source_resource import (
    RawColumnInfo,
    SybaseDriverTypes,
)
from dcs_core.core.datasource.sql_datasource import SQLDataSource


class SybaseDataSource(SQLDataSource):
    def __init__(self, data_source_name: str, data_connection: Dict):
        super().__init__(data_source_name, data_connection)
        self.regex_patterns = {
            "uuid": r"%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%-%[0-9a-fA-F]%",
            "usa_phone": r"%[0-9][0-9][0-9] [0-9][0-9][0-9] [0-9][0-9][0-9][0-9]%",
            "email": r"%[a-zA-Z0-9._%+-]@[a-zA-Z0-9.-]%.[a-zA-Z]%",
            "usa_zip_code": r"[0-9][0-9][0-9][0-9][0-9]%",
            "ssn": r"%[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]%",
            "sedol": r"[B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][B-DF-HJ-NP-TV-XZ0-9][0-9]",
            "lei": r"[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9][0-9]",
            "cusip": r"[0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z]",
            "figi": r"BBG[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]",
            "isin": r"[A-Z][A-Z][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9]",
            "perm_id": r"%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9][0-9][- ]%[0-9][0-9][0-9]%",
        }
        self.sybase_driver_type = SybaseDriverTypes()

    def connect(self) -> Any:
        driver = self.data_connection.get("driver") or "FreeTDS"
        host = self.data_connection.get("host") or ""
        server = self.data_connection.get("server") or ""
        port = self.data_connection.get("port", 5000)
        database = self.data_connection.get("database")
        username = self.data_connection.get("username")
        password = self.data_connection.get("password")
        self._detect_driver_type(driver)

        if self.sybase_driver_type.is_freetds:
            conn_dict = {
                "driver": "FreeTDS",
                "database": database,
                "user": username,
                "password": password,
                "port": port,
                "tds_version": "auto",
            }

            conn_dict["host"] = host or server

            try:
                logger.debug("Attempting FreeTDS connection")
                self.connection = pyodbc.connect(**conn_dict)
                logger.info("Successfully connected to Sybase using FreeTDS")
                return self.connection
            except Exception as e:
                error_msg = f"Failed to connect to Sybase with FreeTDS: {str(e)}"
                logger.error(error_msg)
                raise DataChecksDataSourcesConnectionError(message=error_msg)

        base_params = {
            "DRIVER": self._prepare_driver_string(driver),
            "DATABASE": database,
            "UID": username,
            "PWD": password,
        }

        connection_attempts = []
        if self.sybase_driver_type.is_ase:
            connection_attempts = [
                {
                    "key": "SERVER",
                    "value": host,
                    "port": port,
                },  # ASE typically uses SERVER
                {"key": "SERVERNAME", "value": host, "port": port},
                {
                    "key": "HOST",
                    "value": f"{host}:{port}",
                    "port": None,
                },  # Host:Port format
            ]
        else:
            connection_attempts = [
                {"key": "HOST", "value": f"{host}:{port}", "port": None},
                {"key": "HOST", "value": host, "port": port},
                {"key": "SERVER", "value": server, "port": port},
                {"key": "SERVERNAME", "value": server, "port": port},
            ]

        errors = []

        for attempt in connection_attempts:
            if not attempt["value"]:
                continue

            conn_dict = base_params.copy()
            conn_dict[attempt["key"]] = attempt["value"]

            # Handle port configuration
            if attempt["port"] is not None:
                port_configs = [
                    {"PORT": attempt["port"]},
                    {"Server port": attempt["port"]},
                    {},  # Try without explicit port
                ]
            else:
                port_configs = [{}]  # Port is already in the host string

            for port_config in port_configs:
                current_config = conn_dict.copy()
                current_config.update(port_config)

                # Add ASE-specific parameters if driver is ASE
                if self.sybase_driver_type.is_ase:
                    ase_configs = [
                        {},  # Basic config
                        {"NetworkAddress": f"{host},{port}"},  # Alternative format
                        {"ServerName": host},  # Another common ASE parameter
                    ]
                else:
                    ase_configs = [{}]

                for ase_config in ase_configs:
                    final_config = current_config.copy()
                    final_config.update(ase_config)

                    try:
                        logger.debug("Attempting connection ...")
                        self.connection = pyodbc.connect(**final_config)
                        logger.info(
                            "Successfully connected to Sybase using: "
                            f"driver={driver}"
                        )
                        return self.connection
                    except Exception as e:
                        error_msg = "Failed to connect to sybase."
                        logger.debug(error_msg)
                        errors.append(error_msg)
                        continue

        raise DataChecksDataSourcesConnectionError(
            message=f"Failed to connect to Sybase data source with driver {driver}: "
            f"[{'; '.join(errors)}]"
        )

    def _build_base_connection_params(
        self, driver: str, database: str, username: str, password: str
    ) -> Dict[str, Any]:
        """Build base connection parameters dictionary."""
        return {
            "DRIVER": self._prepare_driver_string(driver),
            "DATABASE": database,
            "UID": username,
            "PWD": password,
        }

    def _normalize_driver(self, driver: str) -> str:
        """Normalize driver string by removing braces, spaces, and converting to lowercase."""
        return driver.replace("{", "").replace("}", "").replace(" ", "").strip().lower()

    def _detect_driver_type(self, driver: str) -> None:
        """Detect and set the appropriate driver type."""
        normalized_driver = self._normalize_driver(driver)
        self.sybase_driver_type.is_ase = "adaptive" in normalized_driver
        self.sybase_driver_type.is_iq = "iq" in normalized_driver
        self.sybase_driver_type.is_freetds = "freetds" in normalized_driver

    def _prepare_driver_string(self, driver: str) -> str:
        """Ensure driver string is properly formatted with braces."""
        return f"{{{driver}}}" if not driver.startswith("{") else driver

    def fetchall(self, query):
        return self.connection.cursor().execute(query).fetchall()

    def fetchone(self, query):
        return self.connection.cursor().execute(query).fetchone()

    def qualified_table_name(self, table_name: str) -> str:
        """
        Get the qualified table name
        :param table_name: name of the table
        :return: qualified table name
        """
        if self.schema_name:
            return f"[{self.schema_name}].[{table_name}]"
        return f"[{table_name}]"

    def quote_column(self, column: str) -> str:
        """
        Quote the column name
        :param column: name of the column
        :return: quoted column name
        """
        return f"[{column}]"

    def query_get_row_count(self, table: str, filters: str = None) -> int:
        """
        Get the row count
        :param table: name of the table
        :param filters: optional filter
        """
        qualified_table_name = self.qualified_table_name(table)
        query = f"SELECT COUNT(*) FROM {qualified_table_name}"
        if filters:
            query += f" WHERE {filters}"
        return self.fetchone(query)[0]

    def query_get_table_columns(
        self, table: str, schema: str | None = None
    ) -> RawColumnInfo:
        """
        Get the schema of a table.
        :param table: table name
        :return: RawColumnInfo object containing column information
        """
        schema = schema or self.schema_name
        database = self.database
        rows = None
        if self.sybase_driver_type.is_iq:
            query = (
                f"SELECT c.column_name, d.domain_name AS data_type, "
                f"CASE WHEN d.domain_name IN ('DATE', 'TIME', 'TIMESTAMP') THEN c.scale ELSE NULL END AS datetime_precision, "
                f"CASE WHEN t.name IN ('float') THEN 15 WHEN t.name IN ('real') THEN 7 ELSE c.prec END AS numeric_precision, "
                f"CASE WHEN t.name IN ('float', 'real') THEN NULL ELSE c.scale END AS numeric_scale, "
                f"NULL AS collation_name, c.width AS character_maximum_length "
                f"FROM {database}.SYS.SYSTABLE t "
                f"JOIN {database}.SYS.SYSCOLUMN c ON t.table_id = c.table_id "
                f"JOIN {database}.SYS.SYSDOMAIN d ON c.domain_id = d.domain_id "
                f"JOIN {database}.SYS.SYSUSER u ON t.creator = u.user_id "
                f"WHERE t.table_name = '{table}' "
                f"AND u.user_name = '{schema}'"
            )

        elif self.sybase_driver_type.is_ase:
            query = (
                f"SELECT c.name AS column_name, t.name AS data_type, "
                f"CASE WHEN c.type IN (61, 111) THEN c.prec ELSE NULL END AS datetime_precision, "
                f"CASE WHEN t.name IN ('float') THEN 15 WHEN t.name IN ('real') THEN 7 ELSE c.prec END AS numeric_precision, "
                f"CASE WHEN t.name IN ('float', 'real') THEN NULL ELSE c.scale END AS numeric_scale, "
                f"NULL AS collation_name, c.length AS character_maximum_length "
                f"FROM {database}..sysobjects o "
                f"JOIN {database}..syscolumns c ON o.id = c.id "
                f"JOIN {database}..systypes t ON c.usertype = t.usertype "
                f"JOIN {database}..sysusers u ON o.uid = u.uid "
                f"WHERE o.name = '{table}' "
                f"AND u.name = '{schema}'"
            )
        elif self.sybase_driver_type.is_freetds:
            try:
                ase_query = (
                    f"SELECT c.name AS column_name, t.name AS data_type, "
                    f"CASE WHEN c.type IN (61, 111) THEN c.prec ELSE NULL END AS datetime_precision, "
                    f"CASE WHEN t.name IN ('float') THEN 15 WHEN t.name IN ('real') THEN 7 ELSE c.prec END AS numeric_precision, "
                    f"CASE WHEN t.name IN ('float', 'real') THEN NULL ELSE c.scale END AS numeric_scale, "
                    f"NULL AS collation_name, c.length AS character_maximum_length "
                    f"FROM {database}..sysobjects o "
                    f"JOIN {database}..syscolumns c ON o.id = c.id "
                    f"JOIN {database}..systypes t ON c.usertype = t.usertype "
                    f"JOIN {database}..sysusers u ON o.uid = u.uid "
                    f"WHERE o.name = '{table}' "
                    f"AND u.name = '{schema}'"
                )
                rows = self.fetchall(ase_query)

            except Exception as _:
                iq_query = (
                    f"SELECT c.name AS column_name, t.name AS data_type, "
                    f"CASE WHEN c.type IN (61, 111) THEN c.prec ELSE NULL END AS datetime_precision, "
                    f"CASE WHEN t.name IN ('float') THEN 15 WHEN t.name IN ('real') THEN 7 ELSE c.prec END AS numeric_precision, "
                    f"CASE WHEN t.name IN ('float', 'real') THEN NULL ELSE c.scale END AS numeric_scale, "
                    f"NULL AS collation_name, c.length AS character_maximum_length "
                    f"FROM {database}.dbo.sysobjects o "
                    f"JOIN {database}.dbo.syscolumns c ON o.id = c.id "
                    f"JOIN {database}.dbo.systypes t ON c.usertype = t.usertype "
                    f"JOIN {database}.dbo.sysusers u ON o.uid = u.uid "
                    f"WHERE o.name = '{table}' AND u.name = '{schema}'"
                )
                rows = self.fetchall(iq_query)
        else:
            raise ValueError("Unknown Sybase driver type")
        if not rows:
            rows = self.fetchall(query)
        if not rows:
            raise RuntimeError(
                f"{table}: Table, {schema}: Schema, does not exist, or has no columns"
            )

        column_info = {
            r[0]: RawColumnInfo(
                column_name=self.safe_get(r, 0),
                data_type=self.safe_get(r, 1),
                datetime_precision=self.safe_get(r, 2),
                numeric_precision=self.safe_get(r, 3),
                numeric_scale=self.safe_get(r, 4),
                collation_name=self.safe_get(r, 5),
                character_maximum_length=self.safe_get(r, 6),
            )
            for r in rows
        }
        return column_info

    def query_get_table_indexes(
        self, table: str, schema: str | None = None
    ) -> dict[str, dict]:
        """
        Get index information for a table in Sybase (IQ/ASE/FreeTDS).
        :param table: Table name
        :param schema: Optional schema name
        :return: Dictionary with index details
        """
        schema = schema or self.schema_name
        database = self.database
        rows = None

        if self.sybase_driver_type.is_iq:
            query = (
                "SELECT\n"
                "    t.table_name,\n"
                "    i.index_name,\n"
                "    i.index_type,\n"
                "    c.column_name,\n"
                "    ic.sequence AS column_order\n"
                "FROM\n"
                f"    {database}.sys.systable t\n"
                "JOIN\n"
                f"    {database}.sys.sysindex i ON t.table_id = i.table_id\n"
                "JOIN\n"
                f"    {database}.sys.sysixcol ic ON i.index_id = ic.index_id AND i.table_id = ic.table_id\n"
                "JOIN\n"
                f"    {database}.sys.syscolumn c ON ic.column_id = c.column_id AND ic.table_id = c.table_id\n"
                "JOIN\n"
                f"    {database}.sys.sysuser u ON t.creator = u.user_id\n"
                "WHERE\n"
                "    t.table_type = 'BASE'\n"
                f"    AND t.table_name = '{table}'\n"
                f"    AND u.user_name = '{schema}'\n"
                "    AND i.index_name IS NOT NULL\n"
                "ORDER BY\n"
                "    i.index_name, ic.sequence"
            )
            rows = self.fetchall(query)
        elif self.sybase_driver_type.is_ase:
            query = (
                "SELECT\n"
                "    t.name AS table_name,\n"
                "    i.name AS index_name,\n"
                "    CASE \n"
                "        WHEN i.indid = 1 THEN 'CLUSTERED'\n"
                "        WHEN i.indid > 1 AND i.status & 2048 = 2048 THEN 'UNIQUE'\n"
                "        ELSE 'NONCLUSTERED'\n"
                "    END AS index_type,\n"
                "    c.name AS column_name,\n"
                "    ic.keyno AS column_order\n"
                "FROM\n"
                "    sysobjects t\n"
                "JOIN\n"
                "    sysindexes i ON t.id = i.id\n"
                "JOIN\n"
                "    sysindexkeys ic ON i.id = ic.id AND i.indid = ic.indid\n"
                "JOIN\n"
                "    syscolumns c ON ic.id = c.id AND ic.colid = c.colid\n"
                "JOIN\n"
                "    sysusers u ON t.uid = u.uid\n"
                "WHERE\n"
                "    t.type = 'U'\n"
                f"    AND t.name = '{table}'\n"
                f"    AND u.name = '{schema}'\n"
                "    AND i.name IS NOT NULL\n"
                "ORDER BY\n"
                "    i.name, ic.keyno"
            )
            rows = self.fetchall(query)

        elif self.sybase_driver_type.is_freetds:
            try:
                # Try ASE-compatible query
                ase_query = (
                    f"SELECT\n"
                    f"    o.name AS table_name,\n"
                    f"    i.name AS index_name,\n"
                    f"    CASE\n"
                    f"        WHEN i.indid = 1 THEN 'CLUSTERED'\n"
                    f"        ELSE 'NONCLUSTERED'\n"
                    f"    END AS index_type,\n"
                    f"    index_col(o.name, i.indid, c.colid, o.uid) AS column_name,\n"
                    f"    c.colid AS column_order\n"
                    f"FROM\n"
                    f"    sysobjects o\n"
                    f"JOIN\n"
                    f"    sysindexes i ON o.id = i.id\n"
                    f"JOIN\n"
                    f"    syscolumns c ON c.id = o.id\n"
                    f"WHERE\n"
                    f"    o.type = 'U'\n"
                    f"    AND o.name = '{table}'\n"
                    f"    AND user_name(o.uid) = '{schema}'\n"
                    f"    AND i.name IS NOT NULL\n"
                    f"    AND index_col(o.name, i.indid, c.colid, o.uid) IS NOT NULL\n"
                    f"ORDER BY\n"
                    f"    i.name, c.colid\n"
                )
                rows = self.fetchall(ase_query)
            except Exception as e:
                # Fallback to IQ-style query
                iq_query = (
                    "SELECT\n"
                    "    t.table_name,\n"
                    "    i.index_name,\n"
                    "    i.index_type,\n"
                    "    c.column_name,\n"
                    "    ic.sequence AS column_order\n"
                    "FROM\n"
                    f"    {database}.sys.systable t\n"
                    "JOIN\n"
                    f"    {database}.sys.sysindex i ON t.table_id = i.table_id\n"
                    "JOIN\n"
                    f"    {database}.sys.sysixcol ic ON i.index_id = ic.index_id AND i.table_id = ic.table_id\n"
                    "JOIN\n"
                    f"    {database}.sys.syscolumn c ON ic.column_id = c.column_id AND ic.table_id = c.table_id\n"
                    "JOIN\n"
                    f"    {database}.sys.sysuser u ON t.creator = u.user_id\n"
                    "WHERE\n"
                    "    t.table_type = 'BASE'\n"
                    f"    AND t.table_name = '{table}'\n"
                    f"    AND u.user_name = '{schema}'\n"
                    "    AND i.index_name IS NOT NULL\n"
                    "ORDER BY\n"
                    "    i.index_name, ic.sequence"
                )
                rows = self.fetchall(iq_query)

        else:
            raise ValueError("Unknown Sybase driver type")

        if not rows:
            raise RuntimeError(
                f"No index information found for table '{table}' in schema '{schema}'."
            )

        # Primary key extraction
        pk_columns = []
        if self.sybase_driver_type.is_iq:
            pk_sql = f"sp_iqpkeys {table}, NULL, {schema}"
            pk_rows = self.fetchall(pk_sql)
            if pk_rows:
                raw_columns = pk_rows[0][2]
                pk_columns = [col.strip() for col in raw_columns.split(",")]
        elif self.sybase_driver_type.is_ase:
            pk_sql = (
                "SELECT c.name "
                "FROM sysobjects t "
                "JOIN sysindexes i ON t.id = i.id "
                "JOIN sysindexkeys ic ON i.id = ic.id AND i.indid = ic.indid "
                "JOIN syscolumns c ON ic.id = c.id AND ic.colid = c.colid "
                "JOIN sysusers u ON t.uid = u.uid "
                f"WHERE t.type = 'U' AND t.name = '{table}' AND u.name = '{schema}' "
                "AND i.status & 2 = 2 "
                "ORDER BY ic.keyno"
            )
            pk_rows = self.fetchall(pk_sql)
            pk_columns = [row[0].strip() for row in pk_rows] if pk_rows else []
        elif self.sybase_driver_type.is_freetds:
            try:
                self.connection.autocommit = True
                pk_sql = (
                    f"EXEC sp_pkeys @table_name = '{table}', @table_owner = '{schema}'"
                )
                pk_rows = self.fetchall(pk_sql)
                pk_columns = [row[3].strip() for row in pk_rows] if pk_rows else []
            except Exception as e:
                pk_sql = f"sp_iqpkeys {table}, NULL, {schema}"
                pk_rows = self.fetchall(pk_sql)
                if pk_rows:
                    raw_columns = pk_rows[0][2]
                    pk_columns = [col.strip() for col in raw_columns.split(",")]
        else:
            raise ValueError("Unknown Sybase driver type")

        pk_columns_set = set(pk_columns)

        indexes = {}
        for row in rows:
            index_name = row[1]
            index_type = row[2]
            column_info = {
                "column_name": self.safe_get(row, 3),
                "column_order": self.safe_get(row, 4),
            }
            if index_name not in indexes:
                indexes[index_name] = {"columns": [], "index_type": index_type}
            indexes[index_name]["columns"].append(column_info)

        for index_name, idx in indexes.items():
            index_columns = [col["column_name"].strip() for col in idx["columns"]]
            index_columns_set = set(index_columns)
            idx["is_primary_key"] = pk_columns_set == index_columns_set and len(
                index_columns
            ) == len(pk_columns)

        return indexes

    def query_get_table_names(
        self,
        schema: str | None = None,
        with_view: bool = False,
    ) -> dict:
        """
        Get the list of tables in the database.
        :param schema: optional schema name
        :param with_view: whether to include views
        :return: dictionary with table names and optionally view names
        """
        schema = schema or self.schema_name
        database = self.database
        if with_view:
            type_condition = "IN ('U', 'V')"
        else:
            type_condition = "= 'U'"

        if self.sybase_driver_type.is_iq:
            table_type_condition = (
                "table_type IN ('BASE', 'VIEW')" if with_view else "table_type = 'BASE'"
            )
            query = f"SELECT table_name, table_type FROM {database}.SYS.SYSTABLE WHERE creator = USER_ID('{schema}') AND {table_type_condition}"
        elif self.sybase_driver_type.is_ase:
            query = f"SELECT name AS table_name, type FROM {database}..sysobjects WHERE type {type_condition} AND uid = USER_ID('{schema}')"
        elif self.sybase_driver_type.is_freetds:
            query = f"SELECT name AS table_name, type FROM {database}.dbo.sysobjects WHERE type {type_condition} AND uid = USER_ID('{schema}')"
        else:
            raise ValueError("Unknown Sybase driver type")

        rows = self.fetchall(query)

        if with_view:
            result = {"table": [], "view": []}
            if rows:
                for row in rows:
                    table_name = row[0]
                    table_type = row[1].strip() if row[1] else row[1]

                    if self.sybase_driver_type.is_iq:
                        if table_type == "BASE":
                            result["table"].append(table_name)
                        elif table_type == "VIEW":
                            result["view"].append(table_name)
                    else:  # ASE or FreeTDS
                        if table_type == "U":
                            result["table"].append(table_name)
                        elif table_type == "V":
                            result["view"].append(table_name)
        else:
            result = {"table": []}
            if rows:
                result["table"] = [row[0] for row in rows]

        return result

    def fetch_rows(
        self,
        query: str,
        limit: int = 1,
        with_column_names: bool = False,
        complete_query: Optional[str] = None,
    ) -> Tuple[List, Optional[List[str]]]:
        """
        Fetch rows from the database using pyodbc.

        :param query: SQL query to execute.
        :param limit: Number of rows to fetch.
        :param with_column_names: Whether to include column names in the result.
        :return: Tuple of (rows, column_names or None)
        """
        query = complete_query or f"SELECT TOP {limit} * FROM ({query}) AS subquery"
        cursor = self.connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchmany(limit)

        if with_column_names:
            column_names = [column[0] for column in cursor.description]
            return rows, column_names
        else:
            return rows, None

    def convert_regex_to_sybase_pattern(self, regex_pattern: str) -> str:
        """
        Convert a regex pattern into a Sybase-compatible LIKE pattern.
        """
        sybase_pattern = re.sub(r"([%_])", r"[\1]", regex_pattern)

        sybase_pattern = sybase_pattern.replace(".*", "%")
        sybase_pattern = sybase_pattern.replace(".", "_")
        sybase_pattern = sybase_pattern.replace(".+", "_%")

        sybase_pattern = sybase_pattern.replace("?", "_")

        sybase_pattern = re.sub(
            r"\[([^\]]+)\]", lambda m: f"%[{m.group(1)}]%", sybase_pattern
        )

        sybase_pattern = sybase_pattern.lstrip("^").rstrip("$")

        return sybase_pattern

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
        field = self.quote_column(field)
        if values:
            values_str = ", ".join([f"'{value}'" for value in values])
            validation_query = f"CASE WHEN {field} IN ({values_str}) THEN 1 ELSE 0 END"
        else:
            sybase_pattern = self.convert_regex_to_sybase_pattern(regex_pattern)
            validation_query = (
                f"CASE WHEN {field} LIKE '{sybase_pattern}' THEN 1 ELSE 0 END"
            )

        query = f"""
            SELECT SUM({validation_query}) AS valid_count, COUNT(*) as total_count
            FROM {qualified_table_name}
            {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_percentile(
        self, table: str, field: str, percentile: float, filters: str = None
    ) -> float:
        raise NotImplementedError("Method not implemented for Sybase data source")

    def query_get_all_space_count(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the count of rows where the specified column contains only spaces.
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of rows with only spaces
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        query = f"""
            SELECT COUNT(*) AS space_count
            FROM {qualified_table_name}
            WHERE {field} LIKE '% %' OR {field} LIKE '%' + CHAR(160) + '%'
        """

        if filters:
            query += f" AND {filters}"

        total_query = f"SELECT COUNT(*) AS total_count FROM {qualified_table_name}"
        if filters:
            total_query += f" WHERE {filters}"

        space_count = self.fetchone(query)[0]
        total_count = self.fetchone(total_query)[0]

        if operation == "percent":
            return round((space_count / total_count) * 100, 2) if total_count > 0 else 0

        return space_count if space_count is not None else 0

    def query_get_null_keyword_count(
        self, table: str, field: str, operation: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the count of NULL-like values (specific keywords) in the specified column.
        :param table: table name
        :param field: column name
        :param filters: filter condition
        :return: count of NULL-like keyword values
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        # Query that checks for both NULL and specific NULL-like values
        query = f"""
                SELECT SUM(CASE
                            WHEN {field} IS NULL OR LOWER({field}) IN ('nothing', 'nil', 'null', 'none', 'n/a')
                            THEN 1
                            ELSE 0
                        END) AS null_count, COUNT(*) AS total_count
                FROM {qualified_table_name}
            """
        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)

        if result:
            if operation == "percent":
                return round((result[0] / result[1]) * 100, 2) if result[1] > 0 else 0
            return result[0]

        return 0

    def query_get_string_length_metric(
        self, table: str, field: str, metric: str, filters: str = None
    ) -> Union[int, float]:
        """
        Get the string length metric (max, min, avg) in a column of a table.

        :param table: table name
        :param field: column name
        :param metric: the metric to calculate ('max', 'min', 'avg')
        :param filters: filter condition
        :return: the calculated metric as int for 'max' and 'min', float for 'avg'
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        if metric.lower() == "max":
            sql_function = "MAX(LEN"
        elif metric.lower() == "min":
            sql_function = "MIN(LEN"
        elif metric.lower() == "avg":
            sql_function = "AVG(CAST(LEN(" + field + ") AS FLOAT))"
        else:
            raise ValueError(
                f"Invalid metric '{metric}'. Choose from 'max', 'min', or 'avg'."
            )
        if metric.lower() == "avg":
            query = f"SELECT {sql_function} FROM {qualified_table_name}"
        else:
            query = f"SELECT {sql_function}({field})) FROM {qualified_table_name}"
        if filters:
            query += f" WHERE {filters}"

        result = self.fetchone(query)[0]
        return round(result, 2) if metric.lower() == "avg" else result

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
        field = self.quote_column(field)

        if not regex_pattern and not predefined_regex_pattern:
            raise ValueError(
                "Either regex_pattern or predefined_regex_pattern should be provided"
            )

        if predefined_regex_pattern:
            length_query = None
            pt = self.regex_patterns[predefined_regex_pattern]
            if predefined_regex_pattern == "uuid":
                length_query = f"LEN({field}) = 36"
            elif predefined_regex_pattern == "perm_id":
                length_query = f"LEN({field}) BETWEEN 19 AND 23 "
            elif predefined_regex_pattern == "lei":
                length_query = f"LEN({field}) = 20"
            elif predefined_regex_pattern == "cusip":
                length_query = f"LEN({field}) = 9"
            elif predefined_regex_pattern == "figi":
                length_query = f"LEN({field}) = 12"
            elif predefined_regex_pattern == "isin":
                length_query = f"LEN({field}) = 12"
            elif predefined_regex_pattern == "sedol":
                length_query = f"LEN({field}) = 7"
            elif predefined_regex_pattern == "ssn":
                length_query = f"LEN({field}) = 11"
            elif predefined_regex_pattern == "usa_zip_code":
                query = f"""
                    SELECT
                        SUM(CASE
                            WHEN PATINDEX('%[0-9][0-9][0-9][0-9][0-9]%', CAST({field} AS VARCHAR)) > 0
                            AND (LEN(CAST({field} AS VARCHAR)) = 5 OR LEN(CAST({field} AS VARCHAR)) = 9)
                            THEN 1
                            ELSE 0
                        END) AS valid_count,
                        COUNT(*) AS total_count
                    FROM {qualified_table_name} {filters};
                    """
                result = self.fetchone(query)
                return result[0], result[1]
            if not length_query:
                regex_query = f"PATINDEX('{pt}', {field}) > 0"
            else:
                regex_query = f"PATINDEX('{pt}', {field}) > 0 AND {length_query}"
        else:
            regex_query = self.convert_regex_to_sybase_pattern(regex_pattern)
        query = f"""
            SELECT
                SUM(CASE
                        WHEN {regex_query}
                        THEN 1
                        ELSE 0
                    END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name} {filters}
        """
        result = self.fetchone(query)
        return result[0], result[1]

    def query_get_time_diff(self, table: str, field: str) -> int:
        """
        Get the time difference
        :param table: name of the index
        :param field: field name of updated time column
        :return: time difference in seconds
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        query = f"""
            SELECT TOP 1 {field}
            FROM {qualified_table_name}
            ORDER BY {field} DESC;
        """
        result = self.fetchone(query)
        if result:
            updated_time = result[0]
            if isinstance(updated_time, str):
                updated_time = datetime.strptime(updated_time, "%Y-%m-%d %H:%M:%S.%f")
            return int((datetime.utcnow() - updated_time).total_seconds())
        return 0

    def query_timestamp_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Tuple containing valid count and total count (or percentage)
        """

        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        temp_table_suffix = f"{int(time.time())}_{random.randint(1000, 9999)}"
        extracted_table = f"#extracted_timestamps_{temp_table_suffix}"
        validated_table = f"#validated_timestamps_{temp_table_suffix}"

        if predefined_regex == "timestamp_iso":
            filters_clause = f"WHERE {filters}" if filters else ""

            query = f"""
                -- Extract timestamp components
                SELECT
                    {field},
                    LEFT(CONVERT(VARCHAR, {field}, 120), 4) AS year,       -- Extract year
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 6, 2) AS month,  -- Extract month
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 9, 2) AS day,    -- Extract day
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 12, 2) AS hour,  -- Extract hour
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 15, 2) AS minute, -- Extract minute
                    SUBSTRING(CONVERT(VARCHAR, {field}, 120), 18, 2) AS second  -- Extract second
                INTO {extracted_table}
                FROM {qualified_table_name}
                {filters_clause};

                -- Validate timestamps and calculate the is_valid flag
                SELECT
                    {field},
                    CASE
                        WHEN
                            -- Validate year, month, and day formats
                            year LIKE '[0-9][0-9][0-9][0-9]' AND
                            month LIKE '[0-1][0-9]' AND month BETWEEN '01' AND '12' AND
                            day LIKE '[0-3][0-9]' AND day BETWEEN '01' AND
                                CASE
                                    -- Check for days in each month
                                    WHEN month IN ('01', '03', '05', '07', '08', '10', '12') THEN '31'
                                    WHEN month IN ('04', '06', '09', '11') THEN '30'
                                    WHEN month = '02' THEN
                                        CASE
                                            -- Check for leap years
                                            WHEN (CAST(year AS INT) % 400 = 0 OR (CAST(year AS INT) % 100 != 0 AND CAST(year AS INT) % 4 = 0)) THEN '29'
                                            ELSE '28'
                                        END
                                    ELSE '00' -- Invalid month
                                END AND
                            -- Validate time components
                            hour LIKE '[0-2][0-9]' AND hour BETWEEN '00' AND '23' AND
                            minute LIKE '[0-5][0-9]' AND
                            second LIKE '[0-5][0-9]'
                        THEN 1
                        ELSE 0
                    END AS is_valid
                INTO {validated_table}
                FROM {extracted_table};

                -- Get the counts
                SELECT
                    SUM(is_valid) AS valid_count,
                    COUNT(*) AS total_count
                FROM {validated_table};
            """
            try:
                result = self.fetchone(query)
                valid_count = result[0]
                total_count = result[1]

                return valid_count, total_count
            except Exception as e:
                logger.error(f"Error occurred: {e}")
                return 0, 0
        else:
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

    def query_timestamp_not_in_future_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: regex pattern
        :param filters: filter condition
        :return: Count of valid timestamps not in the future and total count or percentage
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)

        if predefined_regex != "timestamp_iso":
            raise ValueError(f"Unknown predefined regex pattern: {predefined_regex}")

        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            SELECT
                SUM(CASE
                    WHEN
                        -- Validate year, month, day
                        DATEPART(yy, {field}) BETWEEN 1 AND 9999 AND
                        DATEPART(mm, {field}) BETWEEN 1 AND 12 AND
                        DATEPART(dd, {field}) BETWEEN 1 AND
                            CASE
                                WHEN DATEPART(mm, {field}) IN (1, 3, 5, 7, 8, 10, 12) THEN 31
                                WHEN DATEPART(mm, {field}) IN (4, 6, 9, 11) THEN 30
                                WHEN DATEPART(mm, {field}) = 2 THEN
                                    CASE
                                        WHEN DATEPART(yy, {field}) % 400 = 0 OR
                                            (DATEPART(yy, {field}) % 4 = 0 AND DATEPART(yy, {field}) % 100 != 0) THEN 29
                                        ELSE 28
                                    END
                                ELSE 0
                            END AND
                        -- Validate hour, minute, second
                        DATEPART(hh, {field}) BETWEEN 0 AND 23 AND
                        DATEPART(mi, {field}) BETWEEN 0 AND 59 AND
                        DATEPART(ss, {field}) BETWEEN 0 AND 59 AND
                        -- Ensure timestamp is not in the future
                        {field} <= GETDATE()
                    THEN 1
                    ELSE 0
                END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters_clause}
        """

        try:
            result = self.fetchone(query)
            valid_count = result[0]
            total_count = result[1]

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0

    def query_timestamp_date_not_in_future_metric(
        self,
        table: str,
        field: str,
        predefined_regex: str,
        filters: str = None,
    ) -> Union[float, int]:
        """
        :param table: Table name
        :param field: Column name
        :param predefined_regex: The regex pattern to use (e.g., "timestamp_iso")
        :param filters: Optional filter condition
        :return: Tuple containing count of valid dates not in the future and total count
        """
        qualified_table_name = self.qualified_table_name(table)
        field = self.quote_column(field)
        filters_clause = f"WHERE {filters}" if filters else ""

        query = f"""
            SELECT
                SUM(CASE
                    WHEN
                        -- Validate year, month, and day
                        DATEPART(yy, {field}) BETWEEN 1 AND 9999 AND
                        DATEPART(mm, {field}) BETWEEN 1 AND 12 AND
                        DATEPART(dd, {field}) BETWEEN 1 AND
                            CASE
                                WHEN DATEPART(mm, {field}) IN (1, 3, 5, 7, 8, 10, 12) THEN 31
                                WHEN DATEPART(mm, {field}) IN (4, 6, 9, 11) THEN 30
                                WHEN DATEPART(mm, {field}) = 2 THEN
                                    CASE
                                        WHEN DATEPART(yy, {field}) % 400 = 0 OR
                                            (DATEPART(yy, {field}) % 4 = 0 AND DATEPART(yy, {field}) % 100 != 0) THEN 29
                                        ELSE 28
                                    END
                                ELSE 0
                            END AND
                        -- Validate hour, minute, and second
                        DATEPART(hh, {field}) BETWEEN 0 AND 23 AND
                        DATEPART(mi, {field}) BETWEEN 0 AND 59 AND
                        DATEPART(ss, {field}) BETWEEN 0 AND 59 AND
                        -- Ensure the timestamp is not in the future
                        {field} <= GETDATE()
                    THEN 1
                    ELSE 0
                END) AS valid_count,
                COUNT(*) AS total_count
            FROM {qualified_table_name}
            {filters_clause}
        """

        try:
            result = self.fetchone(query)
            valid_count = result[0]
            total_count = result[1]

            return valid_count, total_count
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return 0, 0
