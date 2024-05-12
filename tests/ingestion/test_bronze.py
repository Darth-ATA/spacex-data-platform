import pytest
from unittest.mock import patch
from spacex_data_platform.ingestion.bronze.bronze_data import SpaceXBronze
from datetime import datetime
import pandas as pd


class TestRaw:
    @pytest.fixture
    def raw_data(self) -> str:
        return pd.DataFrame(
            {
                "id": ["case"],
            }
        )

    @patch("pandas.read_json")
    def test_spacex_bronze_creates_bronze_data_with_id_as_index(
        self, mock_read_json, tmp_path, raw_data
    ):
        mock_read_json.return_value = raw_data
        expected_path = f"{tmp_path}/bronze/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.parquet"

        result_path = SpaceXBronze.create_bronze(
            f"{tmp_path}/raw/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.json"
        )
        assert expected_path == result_path

        result = pd.read_parquet(result_path)
        assert result.index.name == "id"
