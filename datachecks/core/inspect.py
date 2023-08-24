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
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Union

import requests
from loguru import logger

from datachecks.core.common.models.configuration import Configuration
from datachecks.core.common.models.metric import (
    DataSourceMetrics,
    IndexMetrics,
    MetricValue,
    TableMetrics,
)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.datasource.sql_datasource import SQLDatasource
from datachecks.core.metric.manager import MetricManager
from datachecks.core.profiling.datasource_profiling import DataSourceProfiling
from datachecks.core.utils.tracking import (
    create_error_event,
    create_inspect_event_json,
    is_tracking_enabled,
    send_event_json,
)
from datachecks.core.utils.utils import truncate_error

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


@dataclass
class InspectOutput:
    metrics: Dict[str, DataSourceMetrics]

    def get_inspect_info(self):
        metrics_count, datasource_count = 0, 0
        table_count, index_count = 0, 0
        for ds_met in self.metrics.values():
            datasource_count = datasource_count + 1
            for table_met in ds_met.table_metrics.values():
                table_count = table_count + 1
                metrics_count = metrics_count + len(list(table_met.metrics.values()))
            for index_met in ds_met.index_metrics.values():
                index_count = index_count + 1
                metrics_count = metrics_count + len(list(index_met.metrics.values()))
        return {
            "metrics_count": metrics_count,
            "datasource_count": datasource_count,
            "table_count": table_count,
            "index_count": index_count,
        }


class Inspect:
    def __init__(
        self,
        configuration: Configuration,
        auto_profile: bool = False,
    ):
        self.configuration = configuration
        self._auto_profile = auto_profile
        self.execution_time_taken = 0
        try:
            self.data_source_manager = DataSourceManager(configuration.data_sources)
            self.data_source_names = self.data_source_manager.get_data_source_names()
            self.metric_manager = MetricManager(
                metric_config=configuration.metrics,
                data_source_manager=self.data_source_manager,
            )
        except Exception as ex:
            logger.error(f"Error while initializing Inspect: {ex}")
            if is_tracking_enabled():
                event_json = create_error_event(
                    exception=ex,
                )
                send_event_json(event_json)
            traceback.print_exc(file=sys.stdout)
            raise ex

    def _base_data_source_metrics(self) -> Dict[str, DataSourceMetrics]:
        data_sources: Dict[str, DataSource] = self.data_source_manager.get_data_sources
        results: Dict[str, DataSourceMetrics] = {}
        for data_source_name in data_sources.keys():
            if data_source_name not in results:
                results[data_source_name] = DataSourceMetrics(
                    data_source=data_source_name,
                    table_metrics={},
                    index_metrics={},
                )
        return results

    def _generate_data_source_profile_metrics(
        self, base_datasource_metrics: Dict[str, DataSourceMetrics]
    ):
        """
        Generate the data source profile metrics
        """
        data_sources: Dict[str, DataSource] = self.data_source_manager.get_data_sources

        # Iterate over all the data sources
        for data_source_name, data_source in data_sources.items():
            if isinstance(data_source, SQLDatasource):
                data_source_metrics: DataSourceMetrics = base_datasource_metrics[
                    data_source_name
                ]
                profiler = DataSourceProfiling(data_source=data_source)
                list_metrics: List[
                    Union[TableMetrics, IndexMetrics]
                ] = profiler.generate()

                # Add the metrics to the data source metrics
                for table_or_index_metrics in list_metrics:
                    # If metrics is a table metrics, add it to the table metrics
                    if isinstance(table_or_index_metrics, TableMetrics):
                        if (
                            table_or_index_metrics.table_name
                            not in data_source_metrics.table_metrics
                        ):
                            data_source_metrics.table_metrics[
                                table_or_index_metrics.table_name
                            ] = table_or_index_metrics
                        else:
                            data_source_metrics.table_metrics[
                                table_or_index_metrics.table_name
                            ].metrics.update(table_or_index_metrics.metrics)

                    # If metrics is an index metrics, add it to the index metrics
                    elif isinstance(table_or_index_metrics, IndexMetrics):
                        if (
                            table_or_index_metrics.index_name
                            not in data_source_metrics.index_metrics
                        ):
                            data_source_metrics.index_metrics[
                                table_or_index_metrics.index_name
                            ] = table_or_index_metrics
                        else:
                            data_source_metrics.index_metrics[
                                table_or_index_metrics.index_name
                            ].metrics.update(table_or_index_metrics.metrics)

    @staticmethod
    def _prepare_results(
        results: List[MetricValue],
        base_datasource_metrics: Dict[str, DataSourceMetrics],
    ):
        """
        prepare_results is a function that prepares the
        results of the metrics into a dictionary of data source metrics
        args:
            results: List[MetricValue]
            base_datasource_metrics: Dict[str, DataSourceMetrics]
        returns:
            Dict[str, DataSourceMetrics]
        """

        for result in results:
            data_source_name = result.data_source
            data_source_metrics: DataSourceMetrics = base_datasource_metrics[
                data_source_name
            ]
            table_name = result.table_name
            index_name = result.index_name
            # If the index name is present, add the result to the index name
            if index_name is not None:
                if index_name not in data_source_metrics.index_metrics:
                    data_source_metrics.index_metrics[index_name] = IndexMetrics(
                        index_name=index_name, data_source=data_source_name, metrics={}
                    )
                data_source_metrics.index_metrics[index_name].metrics[
                    result.identity
                ] = result

            # If the table name is present, add the result to the table name
            if table_name is not None:
                if table_name not in data_source_metrics.table_metrics:
                    data_source_metrics.table_metrics[table_name] = TableMetrics(
                        table_name=table_name, data_source=data_source_name, metrics={}
                    )
                data_source_metrics.table_metrics[table_name].metrics[
                    result.identity
                ] = result

    def run(self) -> InspectOutput:
        """
        This method starts the inspection process.
        """
        start = datetime.now()
        error = None
        inspect_info = None
        try:
            datasource_metrics: Dict[
                str, DataSourceMetrics
            ] = self._base_data_source_metrics()

            # generate metric values for custom metrics
            metric_values: List[MetricValue] = []

            for metric in self.metric_manager.metrics.values():
                metric_value = metric.get_metric_value()
                if metric_value is not None:
                    metric_values.append(metric_value)
            self._prepare_results(metric_values, datasource_metrics)

            # generate metric values for profile metrics
            if self._auto_profile:
                self._generate_data_source_profile_metrics(datasource_metrics)

            output = InspectOutput(metrics=datasource_metrics)
            inspect_info = output.get_inspect_info()
            return output
        except Exception as ex:
            logger.error(f"Error while running inspection: {ex}")
            traceback.print_exc(file=sys.stdout)
            error = ex
        finally:
            end = datetime.now()
            self.execution_time_taken = round((end - start).total_seconds(), 3)
            logger.info(f"Inspection took {self.execution_time_taken} seconds")
            err_message = truncate_error(repr(error))
            if is_tracking_enabled():
                event_json = create_inspect_event_json(
                    runtime_seconds=self.execution_time_taken,
                    inspect_info=inspect_info,
                    error=err_message,
                )
                send_event_json(event_json)
            if error:
                logger.error(error)
