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
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union

import requests
from loguru import logger

from datachecks.core.common.errors import DataChecksRuntimeError
from datachecks.core.common.models.configuration import (
    Configuration,
    MetricStorageConfiguration,
    MetricStorageType,
)
from datachecks.core.common.models.metric import (
    CombinedMetrics,
    DataSourceMetrics,
    IndexMetrics,
    MetricValue,
    TableMetrics,
)
from datachecks.core.common.models.validation import ValidationInfo
from datachecks.core.configuration.configuration_parser_v1 import (
    load_configuration,
    load_configuration_from_yaml_str,
)
from datachecks.core.datasource.base import DataSource
from datachecks.core.datasource.manager import DataSourceManager
from datachecks.core.datasource.sql_datasource import SQLDataSource
from datachecks.core.metric.manager import MetricManager
from datachecks.core.profiling.datasource_profiling import DataSourceProfiling
from datachecks.core.repository.metric_repository import MetricRepository
from datachecks.core.utils.tracking import (
    create_error_event,
    create_inspect_event_json,
    is_tracking_enabled,
    send_event_json,
)
from datachecks.core.utils.utils import truncate_error
from datachecks.core.validation.manager import ValidationManager
from datachecks.integrations.storage.local_file import LocalFileMetricRepository

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


@dataclass
class InspectOutput:
    metrics: Dict[str, Union[DataSourceMetrics, CombinedMetrics]]
    validations: Dict[str, ValidationInfo]

    def get_metric_values(self) -> List[MetricValue]:
        """
        This method returns the list of metric values
        """
        metric_values: List[MetricValue] = []
        for ds_met in self.metrics.values():
            if isinstance(ds_met, DataSourceMetrics):
                for table_met in ds_met.table_metrics.values():
                    for metric in table_met.metrics.values():
                        metric_values.append(metric)
                for index_met in ds_met.index_metrics.values():
                    for metric in index_met.metrics.values():
                        metric_values.append(metric)
            else:
                for metric in ds_met.metrics.values():
                    metric_values.append(metric)
        return metric_values

    def get_inspect_info(self):
        metrics_count, datasource_count, combined_metrics_count = 0, 0, 0
        table_count, index_count = 0, 0
        for ds_met in self.metrics.values():
            if isinstance(ds_met, DataSourceMetrics):
                datasource_count = datasource_count + 1
                for table_met in ds_met.table_metrics.values():
                    table_count = table_count + 1
                    metrics_count = metrics_count + len(
                        list(table_met.metrics.values())
                    )
                for index_met in ds_met.index_metrics.values():
                    index_count = index_count + 1
                    metrics_count = metrics_count + len(
                        list(index_met.metrics.values())
                    )
            else:
                metrics_count += 1
        return {
            "metrics_count": metrics_count,
            "datasource_count": datasource_count,
            "table_count": table_count,
            "index_count": index_count,
        }


class Inspect:
    def __init__(
        self,
        configuration: Optional[Configuration] = None,
        #  auto_profile: bool = False, # Disabled for now
    ):
        if configuration is None:
            self.configuration = Configuration()
        else:
            self.configuration = configuration

        self.data_source_manager = DataSourceManager(self.configuration)
        self.validation_manager = ValidationManager(
            application_configs=self.configuration,
            data_source_manager=self.data_source_manager,
        )

        # self._auto_profile = auto_profile # Disabled for now
        self.execution_time_taken = 0
        self.is_storage_enabled = False

    def _initiate_storage(
        self, metric_storage_config: MetricStorageConfiguration
    ) -> MetricRepository:
        if metric_storage_config.type == MetricStorageType.LOCAL_FILE:
            return LocalFileMetricRepository(self.configuration.storage.params.path)
        else:
            raise DataChecksRuntimeError(
                f"Unsupported storage type: {self.configuration.storage.type}"
            )

    def _base_data_source_metrics(self) -> Dict[str, DataSourceMetrics]:
        """
        This method generates the base data source metrics.
        This is an empty data source metrics with the data source name
        """
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

    @staticmethod
    def _prepare_results(
        results: List[MetricValue],
        datasource_metrics: Dict[str, DataSourceMetrics] = None,
        combined_metrics: Dict[str, CombinedMetrics] = None,
    ):
        """
        prepare_results is a function that prepares the
        results of the metrics into a dictionary of data source metrics
        args:
            results: List[MetricValue]
            base_datasource_metrics: Dict[str, DataSourceMetrics]

        returns:
            Dict[str, Union[DataSourceMetrics,CombinedMetrics]]
        """
        if datasource_metrics is not None:
            for result in results:
                data_source_name = result.data_source
                data_source_metrics: DataSourceMetrics = datasource_metrics[
                    data_source_name
                ]
                table_name = result.table_name
                index_name = result.index_name
                # If the index name is present, add the result to the index name
                if index_name is not None:
                    if index_name not in data_source_metrics.index_metrics:
                        data_source_metrics.index_metrics[index_name] = IndexMetrics(
                            index_name=index_name,
                            data_source=data_source_name,
                            metrics={},
                        )
                    data_source_metrics.index_metrics[index_name].metrics[
                        result.identity
                    ] = result

                # If the table name is present, add the result to the table name
                if table_name is not None:
                    if table_name not in data_source_metrics.table_metrics:
                        data_source_metrics.table_metrics[table_name] = TableMetrics(
                            table_name=table_name,
                            data_source=data_source_name,
                            metrics={},
                        )
                    data_source_metrics.table_metrics[table_name].metrics[
                        result.identity
                    ] = result

        else:
            for result in results:
                expression = result.expression
                combined_metrics[expression] = CombinedMetrics(
                    expression=expression, metrics={result.identity: result}
                )

    def add_configuration_yaml_file(self, file_path: str):
        load_configuration(
            configuration_path=file_path, configuration=self.configuration
        )
        self.validation_manager.set_validation_configs(self.configuration.validations)

    def add_validations_yaml_str(self, yaml_str: str):
        configuration = load_configuration_from_yaml_str(yaml_string=yaml_str)
        self.configuration.validations = configuration.validations

    def add_spark_session(self, spark_session, data_source_name: str = "spark_df"):
        pass

    def run(self) -> InspectOutput:
        """
        This method starts the inspection process.
        """
        start = datetime.now()
        error = None
        inspect_info = None
        try:
            self.data_source_manager.connect()
            self.validation_manager.build_validations()

            # Initiate the data source metrics
            metric_manager = MetricManager(
                metric_config=self.configuration.metrics,
                data_source_manager=self.data_source_manager,
            )
            datasource_metrics: Dict[
                str, DataSourceMetrics
            ] = self._base_data_source_metrics()
            combined_metrics: Dict[str, CombinedMetrics] = {}
            # generate metric values for custom metrics
            metric_values: List[MetricValue] = []
            combined_metric_values: List[MetricValue] = []

            # generate metric values for dataset metrics and populate the datasource_metrics
            for metric in metric_manager.metrics.values():
                metric_value = metric.get_metric_value()
                if metric_value is not None:
                    metric_values.append(metric_value)
            self._prepare_results(metric_values, datasource_metrics=datasource_metrics)

            # generate metric values for combined metrics and populate the combined_metrics
            for combined_metric in metric_manager.combined.values():
                metric_value = combined_metric.get_metric_value(
                    metric_values=metric_values
                )
                if metric_value is not None:
                    combined_metric_values.append(metric_value)
            self._prepare_results(
                combined_metric_values, combined_metrics=combined_metrics
            )

            validation_infos: Dict[str, ValidationInfo] = {}

            for datasource, _ in self.validation_manager.get_validations.items():
                for dataset, _ in self.validation_manager.get_validations[
                    datasource
                ].items():
                    for _, validation in self.validation_manager.get_validations[
                        datasource
                    ][dataset].items():
                        validation_info = validation.get_validation_info()
                        validation_infos[
                            validation.get_validation_identity()
                        ] = validation_info

            output = InspectOutput(
                metrics={**datasource_metrics, **combined_metrics},
                validations=validation_infos,
            )
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
