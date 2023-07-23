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
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

from opensearchpy import OpenSearch
from sqlalchemy import URL, create_engine, text

NUMBER_OF_DATA = 1000


@dataclass
class TestData:
    id: int
    name: str = None
    category: str = None
    is_valid: bool = True
    price: int = None
    last_updated: datetime = None


CATEGORIES = ["HAT", "OIL", "FILTER", "BATTERIES", "HARDWARE"]

url_content = URL.create(
    drivername="postgresql",
    username="postgres",
    password="postgres",
    host="127.0.0.1",
    port=5431,
    database="dc_db",
)

client = OpenSearch(
    hosts=[{"host": "127.0.0.1", "port": 9201}],
    http_auth=("admin", "admin"),
    use_ssl=True,
    verify_certs=False,
    ca_certs=False,
)
engine_content = create_engine(url_content)

content_connection = engine_content.connect()


def generate_data_content(number_of_data: int):
    content_connection.execute(text("DROP TABLE IF EXISTS table_1"))
    content_connection.commit()

    for i in range(number_of_data):
        d = TestData(
            id=i + 2000,
            name=f"name_{i}",
            category=random.choice(CATEGORIES),
            is_valid=bool(random.getrandbits(1)),
            price=random.randint(1, 1000),
        )
        try:
            content_connection.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS table_1 (
                    id int,
                    name varchar(255),
                    category varchar(255),
                    is_valid boolean,
                    price int,
                    PRIMARY KEY (id)
                    )
            """
                )
            )
            content_connection.commit()
            content_connection.execute(
                text(
                    """
                INSERT INTO table_1 (id, name, category, is_valid, price)
                VALUES (:id, :name, :category, :is_valid, :price)
                """
                ),
                {
                    "id": d.id,
                    "name": d.name,
                    "category": d.category,
                    "is_valid": d.is_valid,
                    "price": d.price,
                },
            )
            content_connection.commit()
        except Exception as e:
            raise e


def generate_open_search(number_of_data: int):
    client.indices.delete(index="category_tabel", ignore=[400, 404])
    client.indices.create(
        index="category_tabel",
        ignore=400,
        body={
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "category": {"type": "text"},
                    "is_valid": {"type": "boolean"},
                    "price": {"type": "integer"},
                    "last_updated": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis",
                    },
                }
            }
        },
    )
    for i in range(number_of_data):
        d = TestData(
            id=i,
            name=f"name_{i}",
            category=random.choice(CATEGORIES),
            is_valid=bool(random.getrandbits(1)),
            price=random.randint(1, 1000),
            last_updated=datetime.now() - timedelta(days=random.randint(1, 1000)),
        )
        client.index(
            index="category_tabel",
            body={
                "name": d.name,
                "category": d.category,
                "is_valid": d.is_valid,
                "price": d.price,
                "last_updated": d.last_updated.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )


if __name__ == "__main__":
    generate_data_content(1000)
    generate_open_search(300)
