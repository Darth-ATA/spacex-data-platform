"""Module to get data from SpaceX API and store it in a file"""

import requests
import os
from datetime import datetime


class ApiSpaceXData:
    """Class to get data from SpaceX API and store it in a file"""

    def __init__(self, raw_data_path: str = "data/raw"):
        self.url = "https://api.spacexdata.com/v5/launches"
        self.raw_path = raw_data_path

    def get_data_and_store(self) -> str:
        """Get data from SpaceX API and store it in a file

        Returns:
            str: Path to the stored file
        """
        response = requests.get(self.url)

        data_path = f"{self.raw_path}/{datetime.now().strftime('%Y_%m_%d__%H_%M%_S')}/spacex_data.json"

        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        with open(data_path, "w") as file:
            file.write(response.text)
        return self.raw_path
