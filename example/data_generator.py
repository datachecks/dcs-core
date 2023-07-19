from dataclasses import dataclass
import random
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


CATEGORIES = ["HAT", "OIL", "FILTER", "BATTERIES", "HARDWARE"]

url_content = URL.create(
    drivername="postgresql",
    username="postgres",
    password="changeme",
    host="127.0.0.1",
    port=5431,
    database="postgres"
)
url_staging = URL.create(
    drivername="postgresql",
    username="postgres",
    password="changeme",
    host="127.0.0.1",
    port=5430,
    database="postgres"
)
client = OpenSearch(
            hosts=[{'host': "127.0.0.1", 'port': 9201}],
            http_auth=("admin", "admin"),
            use_ssl=True,
            verify_certs=False,
            ca_certs=False
        )
engine_content = create_engine(url_content)
engine_staging = create_engine(url_staging)

content_connection = engine_content.connect()
staging_connection = engine_staging.connect()


def generate_data_content(number_of_data: int):
    for i in range(number_of_data):
        d = TestData(
            id=i + 2000,
            name=f"name_{i}",
            category=random.choice(CATEGORIES),
            is_valid=bool(random.getrandbits(1))
        )
        try:
            content_connection \
                .execute(text("""
                INSERT INTO table_1 (id, name, category, is_valid)  VALUES (:id, :name, :category, :is_valid)
                """), {"id": d.id, "name": d.name, "category": d.category, "is_valid": d.is_valid})
            content_connection.commit()
        except Exception as e:
            raise e


def generate_data_staging(number_of_data: int):
    for i in range(number_of_data):
        d = TestData(
            id=i + 2000,
            name=f"name_{i}",
            category=random.choice(CATEGORIES),
            is_valid=bool(random.getrandbits(1))
        )
        try:
            staging_connection \
                .execute(text("""
                INSERT INTO table_2 (id, name, category, is_valid) VALUES (:id, :name, :category, :is_valid)
                """), {"id": d.id, "name": d.name, "category": d.category, "is_valid": d.is_valid})
            staging_connection.commit()
        except Exception as e:
            print(e)


def generate_open_search(number_of_data: int):
    for i in range(number_of_data):
        d = TestData(
            id=i,
            name=f"name_{i}",
            category=random.choice(CATEGORIES),
            is_valid=bool(random.getrandbits(1))
        )
        client.index(index="category_tabel", body={"name": d.name, "category": d.category, "is_valid": d.is_valid})


if __name__ == "__main__":
    generate_data_content(100)
    generate_data_staging(200)
    generate_open_search(300)
