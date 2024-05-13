"""Interface to create the silver layer data ETLs"""

from abc import ABC, abstractmethod

import pandas as pd


class SilverDataInterface(ABC):
    """Interface to create the silver layer data ETLs"""

    @abstractmethod
    def store(self, bronze_data_path: str, df: pd.DataFrame) -> str:
        """Store the silver layer data"""
        pass

    @abstractmethod
    def run(
        self,
        bronze_data_path: str,
    ) -> str:
        """Run the silver layer data ETLs"""
        pass
