"""Main module to run the data-platform"""

from spacex_data_platform.ingestion.raw.api_spacex_data import ApiSpaceXData
from spacex_data_platform.ingestion.bronze.bronze_data import SpaceXBronze
import logging


class Main:
    """Main class to run the data-platform"""

    def __init__(self):
        self._raw = ApiSpaceXData()
        self._bronze = SpaceXBronze()

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


main = Main()
main.run()
