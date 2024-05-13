from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from freezegun import freeze_time

from spacex_data_platform.ingestion.constants import DATE_FORMAT, SPACEX_PROVIDER_CODE
from spacex_data_platform.ingestion.silver.cores_data import SpaceXCores
from spacex_data_platform.ingestion.silver.schemas.cores_flights import (
    CoresFlightsSchema,
)
from tests.ingestion.test_bronze import FROZEN_CREATE_DATE


class TestSpaceXCores:
    @freeze_time(FROZEN_CREATE_DATE)
    @pytest.fixture
    def bronzified_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "id": ["failure_case", "success_case"],
                "cores": [
                    [
                        {
                            "core": "5e9e289df35918033d3b2623",
                            "flight": 1.0,
                            "gridfins": False,
                            "landing_attempt": False,
                            "landing_success": None,
                            "landing_type": None,
                            "landpad": None,
                            "legs": False,
                            "reused": False,
                        }
                    ],
                    [
                        {
                            "core": "5f57c54a0622a633027900a1",
                            "flight": 7.0,
                            "gridfins": True,
                            "landing_attempt": True,
                            "landing_success": True,
                            "landing_type": "ASDS",
                            "landpad": "5e9e3032383ecb6bb234e7ca",
                            "legs": True,
                            "reused": True,
                        }
                    ],
                ],
                "create_date": datetime.now().strftime(DATE_FORMAT),
                "provider_code": SPACEX_PROVIDER_CODE,
            }
        )

    @pytest.fixture
    def cores_flights_data(self) -> pd.DataFrame:
        data = pd.DataFrame(
            {
                "id": ["failure_case", "success_case"],
                "create_date": datetime.now().strftime(DATE_FORMAT),
                "provider_code": SPACEX_PROVIDER_CODE,
                "core": ["5e9e289df35918033d3b2623", "5f57c54a0622a633027900a1"],
                "flight": [1, 7],
                "gridfins": [False, True],
                "landing_attempt": [False, True],
                "landing_success": [None, True],
                "landing_type": [None, "ASDS"],
                "landpad": [None, "5e9e3032383ecb6bb234e7ca"],
                "legs": [False, True],
                "reused": [False, True],
            }
        )
        for col in ["gridfins", "landing_attempt", "landing_success", "legs", "reused"]:
            data[col] = data[col].astype("boolean")
        CoresFlightsSchema.to_schema().validate(data)
        return data

    @freeze_time(FROZEN_CREATE_DATE)
    def test_spacex_cores_creates_cores_flight_data(
        self, bronzified_data, cores_flights_data, tmp_path
    ):
        with patch(
            "spacex_data_platform.ingestion.silver.cores_data.pd.read_parquet"
        ) as read_parquet_mock:
            read_parquet_mock.return_value = bronzified_data
            expected_path = f"{tmp_path}/silver/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/cores_flight.parquet"
            result_path = SpaceXCores.run(
                f"{tmp_path}/bronze/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.parquet"
            )
        assert expected_path == result_path

        result = pd.read_parquet(result_path)
        pd.testing.assert_frame_equal(result, cores_flights_data)
