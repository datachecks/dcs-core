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

from dcs_core.core.common.models.configuration import Configuration
from dcs_core.core.common.models.metric import (
    CombinedMetrics,
    DataSourceMetrics,
    MetricValue,
)
from dcs_core.core.common.models.validation import ValidationInfo
from dcs_core.core.configuration.configuration_parser import (
    load_configuration,
    load_configuration_from_yaml_str,
)
from dcs_core.core.datasource.manager import DataSourceManager
from dcs_core.core.utils.tracking import (
    create_inspect_event_json,
    is_tracking_enabled,
    send_event_json,
)
from dcs_core.core.utils.utils import truncate_error
from dcs_core.core.validation.manager import ValidationManager

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)


@dataclass
class InspectOutput:
    validations: Dict[str, ValidationInfo]
    metrics: Optional[Dict[str, Union[DataSourceMetrics, CombinedMetrics]]] = None

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
        # for ds_met in self.metrics.values():
        #     if isinstance(ds_met, DataSourceMetrics):
        #         datasource_count = datasource_count + 1
        #         for table_met in ds_met.table_metrics.values():
        #             table_count = table_count + 1
        #             metrics_count = metrics_count + len(
        #                 list(table_met.metrics.values())
        #             )
        #         for index_met in ds_met.index_metrics.values():
        #             index_count = index_count + 1
        #             metrics_count = metrics_count + len(
        #                 list(index_met.metrics.values())
        #             )
        #     else:
        #         metrics_count += 1
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

        self.execution_time_taken = 0
        self.is_storage_enabled = False

    def add_configuration_yaml_file(self, file_path: str):
        load_configuration(
            configuration_path=file_path, configuration=self.configuration
        )

    def add_validations_yaml_str(self, yaml_str: str):
        load_configuration_from_yaml_str(
            yaml_string=yaml_str, configuration=self.configuration
        )

    def add_spark_session(self, spark_session, data_source_name: str = "spark_df"):
        self.configuration.add_spark_session(data_source_name, spark_session)

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

            output = InspectOutput(validations=validation_infos)
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
