"""Module to create the silver layer data"""

import os

import pandas as pd
import pandera

from spacex_data_platform.ingestion.silver.schemas.cores_flights import (
    CoresFlightsSchema,
)
from spacex_data_platform.ingestion.silver.silver_data import SilverDataInterface


class SpaceXCores(SilverDataInterface):
    """Class to create the cores flight data from the bronze data"""

    @staticmethod
    @pandera.check_output(CoresFlightsSchema.to_schema())
    def generate_cores_flight_data(bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Generate the cores flight data from the bronze data

        Args:
            bronze_df (pd.DataFrame): bronze data

        Returns:
            pd.DataFrame: cores flight data
        """
        cores_flight_data = bronze_df[
            ["id", "create_date", "provider_code", "cores"]
        ].explode("cores")
        # Convert each key of the dictionary in 'cores' to a separate column
        cores_dict = cores_flight_data["cores"].apply(pd.Series)

        # Concatenate the original DataFrame with the new DataFrame created from the dictionary
        cores_flight_data = pd.concat(
            [cores_flight_data.drop("cores", axis=1), cores_dict], axis=1
        )
        cores_flight_data = cores_flight_data.dropna(subset=["core"])

        return cores_flight_data

    @staticmethod
    def store(bronze_data_path: str, df: pd.DataFrame) -> str:
        """Store the cores flight data in the silver layer

        Args:
            bronze_data_path (str): path of the bronze data
            df (pd.DataFrame): cores flight data

        Returns:
            str: path where the cores flight data is stored
        """
        data_path = bronze_data_path.replace("bronze", "silver").replace(
            "spacex_data", "cores_flight"
        )
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        df.to_parquet(data_path)
        return data_path

    @staticmethod
    def run(
        bronze_data_path: str = "data/bronze/2024_05_09__17_05_33/cores_flight.parquet",
    ) -> str:
        """Get the cores information from the bronze data

        Args:
            bronze_data_path (str, optional): bronze data from which take the cores information. Defaults to "data/bronze/2024_05_09__17_05_33/spacex_data.parquet".

        Returns:
            str: path where the cores information is stored
        """
        df = pd.read_parquet(bronze_data_path)

        cores_flight_df = SpaceXCores.generate_cores_flight_data(df)

        return SpaceXCores.store(bronze_data_path, cores_flight_df)
