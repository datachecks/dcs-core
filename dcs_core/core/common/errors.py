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

ERROR_RUNTIME = "runtime_error"
ERROR_CONFIGURATION = "configuration_error"
ERROR_DATA_SOURCES_CONNECTION = "data_sources_connection_error"
ERROR_METRIC_GENERATION = "metric_generation_error"


class DataChecksRuntimeError(Exception):
    """Raised when there is an error in the configuration file."""

    def __init__(self, message):
        super().__init__(message)
        self.error_code = ERROR_RUNTIME


class DataChecksConfigurationError(Exception):
    """Raised when there is an error in the configuration file."""

    def __init__(self, message):
        super().__init__(message)
        self.error_code = ERROR_CONFIGURATION


class DataChecksDataSourcesConnectionError(Exception):
    """Raised when there is an error in the data sources."""

    def __init__(self, message):
        super().__init__(message)
        self.error_code = ERROR_DATA_SOURCES_CONNECTION


class DataChecksMetricGenerationError(Exception):
    """Raised when there is an error in the metric generation process."""

    def __init__(self, message):
        super().__init__(message)
        self.error_code = ERROR_METRIC_GENERATION
