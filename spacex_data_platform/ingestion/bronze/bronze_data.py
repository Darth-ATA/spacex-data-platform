"""Module to create the bronze layer from the raw data"""

import pandas as pd
import os


class SpaceXBronze:
    """Class to create the bronze layer from the raw data"""

    @staticmethod
    def create_bronze(
        raw_data_path: str = "data/raw/2024_05_09__17_05_33/spacex_data.json",
    ) -> str:
        """Create the bronze layer from the raw data

        Returns:
            pd.DataFrame: DataFrame with the bronze data
        """
        df = pd.read_json(raw_data_path)
        df.set_index("id", inplace=True, verify_integrity=True)
        data_path = raw_data_path.replace("raw", "bronze").replace("json", "parquet")
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        df.to_parquet(data_path)
        return data_path
