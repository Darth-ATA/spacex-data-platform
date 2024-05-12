import pytest
from unittest.mock import patch, Mock
from spacex_data_platform.ingestion.raw.api_spacex_data import ApiSpaceXData
from datetime import datetime


class TestRaw:
    @pytest.fixture
    def json_data(self) -> str:
        return "{'test': 'case'}"

    @patch("requests.get")
    def test_api_spacex_data_get_data_and_store_takes_json_and_save_it_in_raw_folder(
        self, mock_request, tmp_path, json_data
    ):
        expected_path = f"{tmp_path}/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.json"
        mock_response = Mock()
        mock_response.text = json_data
        mock_request.return_value = mock_response
        api_spacex_data = ApiSpaceXData(tmp_path)
        result_path = api_spacex_data.get_data_and_store()
        assert expected_path == result_path