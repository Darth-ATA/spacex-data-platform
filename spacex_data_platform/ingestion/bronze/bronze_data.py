"""Module to create the bronze layer from the raw data"""

import pandas as pd
import os
import logging
from spacex_data_platform.ingestion.constants import DATE_FORMAT, SPACEX_PROVIDER_CODE


class SpaceXBronze:
    """Class to create the bronze layer from the raw data"""

    @staticmethod
    def bronzify(raw_df: pd.DataFrame) -> pd.DataFrame:
        """Bronzify the raw data setting the index and adding `create_date` and `provider_code` columns

        Args:
            raw_df (pd.DataFrame): Raw DataFrame

        Returns:
            pd.DataFrame: DataFrame with `create_date` and `provider_code` columns
        """
        try:
            bronzified_df = raw_df.set_index("id", verify_integrity=True)
            bronzified_df["create_date"] = pd.to_datetime("now").strftime(DATE_FORMAT)
            bronzified_df["provider_code"] = SPACEX_PROVIDER_CODE
            return bronzified_df
        except ValueError as e:
            logging.error(
                "The DataFrame has duplicated values in the index, full trace: %s", e
            )
            raise

    @staticmethod
    def create_bronze(
        raw_data_path: str = "data/raw/2024_05_09__17_05_33/spacex_data.json",
    ) -> str:
        """Create the bronze layer from the raw data

        Returns:
            pd.DataFrame: DataFrame with the bronze data
        """
        df = pd.read_json(raw_data_path)

        data_path = raw_data_path.replace("raw", "bronze").replace("json", "parquet")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        bronzified_df = SpaceXBronze.bronzify(df)
        bronzified_df.to_parquet(data_path)
        return data_path
