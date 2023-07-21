from typing import Dict, Any

from sqlalchemy import create_engine, URL

from datachecks.core.datasource.base import SQLDatasource


class PostgresSQLDatasource(SQLDatasource):

    def __init__(
            self,
            data_source_name: str,
            data_source_properties: Dict
    ):
        super().__init__(data_source_name, data_source_properties)

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        url = URL.create(
            drivername="postgresql",
            username=self.data_connection.get('username'),
            password=self.data_connection.get('password'),
            host=self.data_connection.get('host'),
            port=self.data_connection.get('port'),
            database=self.data_connection.get('database')
        )
        engine = create_engine(url)
        self.connection = engine.connect()
        return self.connection
