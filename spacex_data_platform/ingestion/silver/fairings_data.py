"""Module to create the silver layer data"""

import os

import pandas as pd
import pandera

from spacex_data_platform.ingestion.silver.schemas.fairings import (
    FairingsSchema,
)
from spacex_data_platform.ingestion.silver.silver_data import SilverDataInterface


class SpaceXFairings(SilverDataInterface):
    """Class to create the fairings data from the bronze data"""

    @staticmethod
    @pandera.check_output(FairingsSchema.to_schema())
    def generate_fairings_data(bronze_df: pd.DataFrame) -> pd.DataFrame:
        """Generate the fairings data from the bronze data

        Args:
            bronze_df (pd.DataFrame): bronze data

        Returns:
            pd.DataFrame: fairings data
        """
        # Convert each key of the dictionary in 'cores' to a separate column
        fairings_dict = bronze_df["fairings"].apply(pd.Series)
        fairings_dict = fairings_dict.rename(columns={"ships": "recovery_ships"})

        # Concatenate the original DataFrame with the new DataFrame created from the dictionary
        fairings_data = pd.concat(
            [bronze_df.drop("fairings", axis=1), fairings_dict], axis=1
        )

        schema_columns = list(FairingsSchema.to_schema().columns.keys())
        fairings_data = fairings_data[schema_columns]

        return fairings_data

    @staticmethod
    def store(bronze_data_path: str, df: pd.DataFrame) -> str:
        """Store the fairings data in the silver layer

        Args:
            bronze_data_path (str): path of the bronze data
            df (pd.DataFrame): fairings data

        Returns:
            str: path where the fairings data is stored
        """
        data_path = bronze_data_path.replace("bronze", "silver").replace(
            "spacex_data", "fairings"
        )
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        df.to_parquet(data_path)
        return data_path

    @staticmethod
    def run(
        bronze_data_path: str = "data/bronze/2024_05_09__17_05_33/fairings.parquet",
    ) -> str:
        """Get the fairings information from the bronze data

        Args:
            bronze_data_path (str, optional): bronze data from which take the fairings information. Defaults to "data/bronze/2024_05_09__17_05_33/spacex_data.parquet".

        Returns:
            str: path where the fairings information is stored
        """
        df = pd.read_parquet(bronze_data_path)

        cores_flight_df = SpaceXFairings.generate_fairings_data(df)

        return SpaceXFairings.store(bronze_data_path, cores_flight_df)
