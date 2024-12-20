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


def ibm_db2_dll_files_loader():
    """
    Load the IBM DB2 shared library on Windows.

    Instructions:
    - Download the IBM DB2 client from
      https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information
    - Add a system variable `IBM_DB_HOME` with the path to the extracted folder
      (e.g., C:\Program Files\IBM\SQLLIB).
      Do not include 'bin' in the path.
    """
    if os.name == "nt":
        ibm_db_home = os.getenv("IBM_DB_HOME")

        if ibm_db_home:
            dll_directory = os.path.join(ibm_db_home, "bin")
            os.add_dll_directory(dll_directory)
        else:
            print("IBM_DB_HOME environment variable is not set. Please set to use DB2.")
