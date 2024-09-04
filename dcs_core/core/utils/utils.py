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
import re
from datetime import datetime
from pathlib import Path


def truncate_error(error: str):
    first_line = error.split("\n", 1)[0]
    return re.sub("'(.*?)'", "'***'", first_line)


def ensure_directory_exists(dir_path: str, create_if_not_exists=True):
    dir_path = Path(dir_path)
    if dir_path.exists() and dir_path.is_dir():
        return True
    elif create_if_not_exists:
        dir_path.mkdir(parents=True)
        return True
    return False


def write_to_file(file_path: str, data: str):
    with open(file_path, "w") as file:
        file.write(data)


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)
