"""Main module to run the data-platform"""

import logging

from spacex_data_platform.ingestion.bronze.bronze_data import SpaceXBronze
from spacex_data_platform.ingestion.raw.api_spacex_data import ApiSpaceXData
from spacex_data_platform.ingestion.silver.cores_data import SpaceXCores
from spacex_data_platform.ingestion.silver.fairings_data import SpaceXFairings
from spacex_data_platform.ingestion.silver.silver_data import SilverDataInterface


class Main:
    """Main class to run the data-platform"""

    def __init__(self):
        self._raw = ApiSpaceXData()
        self._bronze = SpaceXBronze()
        self._silver = {
            "space_x_fairings": SpaceXFairings(),
            "space_x_cores": SpaceXCores(),
        }

    def run(self) -> None:
        """Run all the data-platform process:
        - Run the ingestion process
        """
        logging.info("Starting ingestion process")
        raw_data_path = self._raw.get_data_and_store()
        logging.info(f"Raw data stored in {raw_data_path}")
        logging.info("Starting bronze process")
        bronze_data_path = self._bronze.create_bronze(raw_data_path)
        logging.info(f"Bronze data stored in {bronze_data_path}")
        logging.info("Starting silver process")
        for key, value in self._silver.items():
            silver_data: SilverDataInterface = value
            silver_data_path = silver_data.run(bronze_data_path)
            logging.info(f"{key} data stored in {silver_data_path}")
        logging.info("Silver process finished")


main = Main()
main.run()
