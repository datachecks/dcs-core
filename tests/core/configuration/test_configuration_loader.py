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

from dcs_core.core.configuration.config_loader import parse_config


def test_config_env_loader():
    os.environ["DB_USER"] = "test_user"
    os.environ["DB_PASS"] = "test_pass"
    data = """
    data_sources:
      - name: search
        type: opensearch
        connection:
          host: 127.0.0.1
          port: 9201
          username: !ENV ${DB_USER}
          password: !ENV ${DB_PASS}
    """

    p = parse_config(data=data)
    assert p["data_sources"][0]["connection"]["username"] == "test_user"
    assert p["data_sources"][0]["connection"]["password"] == "test_pass"
