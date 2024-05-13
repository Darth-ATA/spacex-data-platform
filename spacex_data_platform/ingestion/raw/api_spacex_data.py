"""Module to get data from SpaceX API and store it in a file"""

import os
from datetime import datetime

import requests


class ApiSpaceXData:
    """Class to get data from SpaceX API and store it in a file"""

    def __init__(
        self,
        url: str = "https://api.spacexdata.com/v5/launches",
        raw_data_path: str = "data/raw",
    ):
        self.url = url
        self.raw_path = raw_data_path

    def store(self, response) -> str:
        """Store the response in a file

        Args:
            response (_type_): Response from the SpaceX API

        Returns:
            str: Path to the stored file
        """
        data_path = f"{self.raw_path}/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.json"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        with open(data_path, "w") as file:
            file.write(response.text)
        return data_path

    def get_data_and_store(self) -> str:
        """Get data from SpaceX API and store it in a file

        Returns:
            str: Path to the stored file
        """
        response = requests.get(self.url)

        return self.store(response)
