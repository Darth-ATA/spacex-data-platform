import pytest
from unittest.mock import patch
from spacex_data_platform.ingestion.bronze.bronze_data import SpaceXBronze
from datetime import datetime
import pandas as pd
from freezegun import freeze_time
from spacex_data_platform.ingestion.constants import DATE_FORMAT, SPACEX_PROVIDER_CODE

FROZEN_CREATE_DATE = datetime.now().strftime(DATE_FORMAT)


class TestRaw:
    @pytest.fixture
    def raw_data(self) -> str:
        return pd.DataFrame(
            {
                "id": ["case"],
            }
        )

    @freeze_time(FROZEN_CREATE_DATE)
    @pytest.fixture
    def bronzified_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": ["case"],
                "create_date": datetime.now().strftime(DATE_FORMAT),
                "provider_code": [SPACEX_PROVIDER_CODE],
            }
        ).set_index("id")

    @freeze_time(FROZEN_CREATE_DATE)
    @patch("pandas.read_json")
    def test_spacex_bronze_creates_bronze_data_with_id_as_index(
        self, mock_read_json, tmp_path, raw_data, bronzified_data
    ):
        mock_read_json.return_value = raw_data
        expected_path = f"{tmp_path}/bronze/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.parquet"

        result_path = SpaceXBronze.create_bronze(
            f"{tmp_path}/raw/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.json"
        )
        assert expected_path == result_path

        result = pd.read_parquet(result_path)
        assert result.index.name == "id"
        pd.testing.assert_frame_equal(result, bronzified_data)

    @patch("pandas.read_json")
    def test_spacex_bronze_with_duplicated_id_returns_error(
        self, mock_read_json, tmp_path, raw_data
    ):
        duplicated_raw_data = pd.concat([raw_data, raw_data])

        mock_read_json.return_value = duplicated_raw_data
        with pytest.raises(ValueError):
            SpaceXBronze.create_bronze(
                f"{tmp_path}/raw/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.json"
            )
