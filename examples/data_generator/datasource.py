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

import json
import os
from typing import Any

from elasticsearch import Elasticsearch
from loguru import logger
from opensearchpy import OpenSearch
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from .datasets import DataSetName, get_dataset_df_from_url


class DataSource:
    def __init__(self):
        self._connection = self.__create_connection()

    def __create_connection(self) -> Any:
        pass

    def load_dataset(self, dataset: DataSetName):
        tabel_name = f"dcs_{dataset.value}"
        try:
            response = get_dataset_df_from_url(dataset).to_sql(
                tabel_name, self._connection.engine, if_exists="replace", index=True
            )
            logger.info(
                f"Loaded dataset {dataset.value} into {tabel_name} with {response} records"
            )
        except Exception as e:
            logger.error(
                f"Failed to load dataset {dataset.value} into {tabel_name} with error {e}"
            )


class DataBricksDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self) -> Any:
        host = os.environ.get("databricks.host")
        token = os.environ.get("databricks.token")
        port = os.environ.get("databricks.port", 443)
        schema = os.environ.get("databricks.schema")
        http_path = os.environ.get("databricks.http_path")
        catalog = os.environ.get("databricks.catalog")

        url_staging_db = URL.create(
            "databricks",
            username="token",
            password=token,
            host=host,
            port=port,
            database=schema,
            query={"http_path": http_path, "catalog": catalog},
        )
        engine = create_engine(url_staging_db, echo=True)
        return engine.connect()


class BigQueryDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self) -> Any:
        project_id = os.environ.get("bigquery.project_id")
        dataset_id = os.environ.get("bigquery.dataset_id")
        credentials_base64 = os.environ.get("bigquery.credentials_base64")
        engine = create_engine(
            f"bigquery://{project_id}/{dataset_id}",
            credentials_base64=credentials_base64,
        )
        return engine.connect()


class PGSqlDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self):
        url_staging_db = URL.create(
            drivername="postgresql",
            username=os.environ.get("pgsql.user"),
            password=os.environ.get("pgsql.pass"),
            host=os.environ.get("pgsql.host"),
            port=os.environ.get("pgsql.port"),
            database=os.environ.get("pgsql.database"),
        )
        engine = create_engine(
            url_staging_db,
            connect_args={"options": f"-csearch_path={os.environ.get('pgsql.schema')}"},
        )
        return engine.connect()


class MySqlDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self):
        db_url = URL.create(
            drivername="mysql+pymysql",
            username=os.environ.get("mysql.user"),
            password=os.environ.get("mysql.pass"),
            host=os.environ.get("mysql.host"),
            port=os.environ.get("mysql.port"),
            database=os.environ.get("mysql.database"),
        )
        engine = create_engine(
            db_url,
        )
        return engine.connect()


class OpenSearchDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self):
        auth = (
            os.environ.get("opensearch.user"),
            os.environ.get("opensearch.pass"),
        )
        host = os.environ.get("opensearch.host")
        port = int(os.environ.get("opensearch.port"))
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
            ca_certs=False,
        )
        if not self.client.ping():
            raise Exception("Failed to connect to OpenSearch data source")
        return self.client

    @staticmethod
    def __rec_to_actions(df, index_name):
        for record in df.to_dict(orient="records"):
            yield '{ "index" : { "_index" : "%s"}}' % index_name
            yield json.dumps(record, default=int)

    def load_dataset(self, dataset: DataSetName):
        index_name = f"dcs_{dataset.value}"
        df = get_dataset_df_from_url(dataset)
        self._connection.indices.delete(index=index_name, ignore=[400, 404])
        self._connection.bulk(self.__rec_to_actions(df, index_name))


class ElasticSearchDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self):
        auth = (
            os.environ.get("elasticsearch.user", None),
            os.environ.get("elasticsearch.pass", None),
        )
        host = os.environ.get("elasticsearch.host")
        port = int(os.environ.get("elasticsearch.port"))
        self.client = Elasticsearch(
            hosts=[{"host": host, "port": port, "scheme": "http"}],
            # basic_auth=auth,
        )
        if not self.client.ping():
            raise Exception("Failed to connect to elasticsearch data source")
        return self.client

    @staticmethod
    def __rec_to_actions(df, index_name):
        for record in df.to_dict(orient="records"):
            yield '{ "index" : { "_index" : "%s"}}' % index_name
            yield json.dumps(record, default=int)

    def load_dataset(self, dataset: DataSetName):
        index_name = f"dcs_{dataset.value}"
        df = get_dataset_df_from_url(dataset)
        self._connection.indices.delete(index=index_name, ignore=[400, 404])
        self._connection.bulk(self.__rec_to_actions(df, index_name))


class RedShiftDataSource(DataSource):
    def __init__(self):
        super().__init__()
        self._connection = self.__create_connection()

    def __create_connection(self) -> Any:
        """
        Connect to the data source
        """

        url = URL.create(
            drivername="redshift+psycopg2",
            username=os.environ.get("redshift.user"),
            password=os.environ.get("redshift.pass"),
            host=os.environ.get("redshift.host"),
            port=os.environ.get("redshift.port"),
            database=os.environ.get("redshift.database"),
        )
        engine = create_engine(url, echo=True)
        return engine.connect()
