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

import datetime as dt
from datetime import datetime
from enum import Enum

import pandas as pd
from pandas import DataFrame


class DataSetName(str, Enum):
    IRIS = "iris"
    DIAMOND = "diamond"
    HEALTH_EXP = "health_exp"
    SALARY_JOBS = "salary_jobs"


dataset_urls = {
    DataSetName.IRIS: "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv",
    DataSetName.DIAMOND: "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/diamonds.csv",
    DataSetName.HEALTH_EXP: "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/healthexp.csv",
    DataSetName.SALARY_JOBS: "https://raw.githubusercontent.com/plotly/datasets/master/salaries-ai-jobs-net.csv",
}


def create_date_range_df(number_of_rows: int) -> DataFrame:
    date_range = pd.date_range(
        start=datetime.now(),
        end=datetime.now() - dt.timedelta(days=5),
        periods=number_of_rows,
    )
    date_range_df = pd.DataFrame(date_range, columns=["date"])

    return date_range_df.squeeze()


def get_dataset_df_from_url(dataset_name: DataSetName) -> DataFrame:
    csv = pd.read_csv(dataset_urls[dataset_name])
    date_series = create_date_range_df(number_of_rows=len(csv))
    csv["timestamp"] = date_series
    return csv
