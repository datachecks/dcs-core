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

from abc import ABC
from typing import Any, Dict


class DataSource(ABC):
    """
    Abstract class for data sources
    """

    NUMERIC_PYTHON_TYPES_FOR_PROFILING = ["int", "float"]
    TEXT_PYTHON_TYPES_FOR_PROFILING = ["str"]

    def __init__(self, data_source_name: str, data_connection: Dict):
        self._data_source_name: str = data_source_name
        self.data_connection: Dict = data_connection

    @property
    def data_source_name(self) -> str:
        """
        Get the data source name
        """
        return self._data_source_name

    def connect(self) -> Any:
        """
        Connect to the data source
        """
        raise NotImplementedError("connect method is not implemented")

    def is_connected(self) -> bool:
        """
        Check if the data source is connected
        """
        raise NotImplementedError("is_connected method is not implemented")

    def close(self):
        """
        Close the connection
        """
        raise NotImplementedError("close_connection method is not implemented")
