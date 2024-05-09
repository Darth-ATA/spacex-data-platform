"""Main module to run the data-platform"""

from spacex_data_platform.ingestion.raw.api_spacex_data import ApiSpaceXData
import logging


class Main:
    """Main class to run the data-platform"""

    def __init__(self):
        self._raw = ApiSpaceXData()

    def run(self) -> None:
        """Run all the data-platform process:
        - Run the ingestion process
        """
        logging.info("Starting ingestion process")
        raw_data_path = self._raw.get_data_and_store()
        logging.info(f"Data stored in {raw_data_path}")


main = Main()
main.run()
