from abc import ABC, abstractmethod
from typing import Dict


class MetricLogger(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def log(
            self,
            metric_name: str,
            metric_value: float,
            metric_tags: Dict[str, str] = None
    ):
        """
        Log a metric to the logger
        :param metric_name:
        :param metric_value:
        :param metric_tags:
        :return:
        """
        pass
