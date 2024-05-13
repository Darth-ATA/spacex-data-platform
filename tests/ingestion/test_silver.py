from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from freezegun import freeze_time

from spacex_data_platform.ingestion.constants import DATE_FORMAT, SPACEX_PROVIDER_CODE
from spacex_data_platform.ingestion.silver.cores_data import SpaceXCores
from spacex_data_platform.ingestion.silver.fairings_data import SpaceXFairings
from spacex_data_platform.ingestion.silver.schemas.cores_flights import (
    CoresFlightsSchema,
)
from spacex_data_platform.ingestion.silver.schemas.fairings import (
    FairingsSchema,
)
from tests.ingestion.test_bronze import FROZEN_CREATE_DATE


@freeze_time(FROZEN_CREATE_DATE)
@pytest.fixture
def bronzified_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["failure_case", "success_case"],
            "fairings": [
                {
                    "recovered": False,
                    "recovery_attempt": False,
                    "reused": False,
                    "ships": [],
                },
                {
                    "recovered": True,
                    "recovery_attempt": True,
                    "reused": True,
                    "ships": ["5ea6ed2e080df4000697c909", "5ea6ed2f080df4000697c90c"],
                },
            ],
            "links": [
                {
                    "article": None,
                    "flickr": {
                        "original": [
                            "https://live.staticflickr.com/65535/50428228397_6151927733_o.jpg",
                            "https://live.staticflickr.com/65535/50427359318_67b3397892_o.jpg",
                            "https://live.staticflickr.com/65535/50428050591_36defbe958_o.jpg",
                        ],
                        "small": [],
                    },
                    "patch": {
                        "large": "https://images2.imgbox.com/79/1f/hBdiixIW_o.png",
                        "small": "https://images2.imgbox.com/3b/c3/kd7H9FTQ_o.png",
                    },
                    "presskit": None,
                    "reddit": {
                        "campaign": "https://www.reddit.com/r/spacex/comments/i63bst/starlink_general_discussion_and_deployment_thread/",
                        "launch": "https://www.reddit.com/r/spacex/comments/iu0vtg/rspacex_starlink12_official_launch_discussion/",
                        "media": "https://www.reddit.com/r/spacex/comments/iudifm/rspacex_starlink12_media_thread_photographer/",
                        "recovery": None,
                    },
                    "webcast": "https://youtu.be/UZkaE_9zwQQ",
                    "wikipedia": "https://en.wikipedia.org/wiki/Starlink",
                    "youtube_id": "UZkaE_9zwQQ",
                },
                {},
            ],
            "static_fire_date_utc": [
                "2020-03-13T18:37:00.000Z",
                "2020-07-11T17:58:00.000Z",
            ],
            "static_fire_date_unix": ["1584124620.0", "1594490280.0"],
            "net": False,
            "window": 0,
            "rocket": "5e9d0d95eda69973a809d1ec",
            "success": [0, 1],
            "failures": [
                [
                    {
                        "altitude": 40.0,
                        "reason": "helium tank overpressure lead to the second stage LOX tank explosion",
                        "time": 139,
                    }
                ],
                None,
            ],
            "details": ["Failed Mission", "Success Mission"],
            "crew": [
                [],
                [
                    {
                        "crew": "5f7f1543bf32c864a529b23e",
                        "role": "Mission Specialist 2",
                    },
                    {
                        "crew": "5f7f158bbf32c864a529b23f",
                        "role": "Mission Specialist 1",
                    },
                    {"crew": "5f7f15d5bf32c864a529b240", "role": "Pilot"},
                    {"crew": "5f7f1614bf32c864a529b241", "role": "Commander"},
                ],
            ],
            "ships": [
                [],
                [
                    "5ea6ed2f080df4000697c910"
                    "5ee68c683c228f36bd5809b5"
                    "5ea6ed2f080df4000697c90c"
                    "5ea6ed2e080df4000697c909"
                    "5ea6ed2f080df4000697c90b"
                ],
            ],
            "capsules": [[], ["5fbb0f8fec55b34eb9f35c14"]],
            "payloads": [
                ["5eb0e4d3b6c3bb0006eeb262"],
                [
                    "605b4bfcaa5433645e37d048"
                    "609f48374a12e4692eae4667"
                    "609f49c64a12e4692eae4668"
                ],
            ],
            "launchpad": ["5e9e4502f509092b78566f87", "5e9e4501f509094ba4566f84"],
            "flight_number": [1, 2],
            "name": [
                "Failure Case",
                "Success Case",
            ],
            "date_utc": [
                "2020-11-25T02:13:00.000Z",
                "2020-11-25T02:13:00.000Z",
            ],
            "date_unix": ["1606270380", "1606270380"],
            "date_local": ["2020-11-24T21:13:00-05:00", "2020-11-24T21:13:00-05:00"],
            "date_precision": "hour",
            "upcoming": False,
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
            "auto_update": True,
            "tbd": False,
            "launch_library_id": None,
            "create_date": datetime.now().strftime(DATE_FORMAT),
            "provider_code": SPACEX_PROVIDER_CODE,
        }
    )


class TestSpaceXCores:
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


class TestSpaceXFires:
    @pytest.fixture
    def fairings_data(self) -> pd.DataFrame:
        data = pd.DataFrame(
            {
                "id": ["failure_case", "success_case"],
                "provider_code": SPACEX_PROVIDER_CODE,
                "create_date": datetime.now().strftime(DATE_FORMAT),
                "recovered": [False, True],
                "recovery_attempt": [False, True],
                "reused": [False, True],
                "recovery_ships": [
                    [],
                    ["5ea6ed2e080df4000697c909", "5ea6ed2f080df4000697c90c"],
                ],
                "static_fire_date_utc": [
                    "2020-03-13T18:37:00.000Z",
                    "2020-07-11T17:58:00.000Z",
                ],
                "net": False,
                "window": 0,
                "rocket": "5e9d0d95eda69973a809d1ec",
                "success": [0, 1],
                "details": ["Failed Mission", "Success Mission"],
                "ships": [
                    [],
                    [
                        "5ea6ed2f080df4000697c910"
                        "5ee68c683c228f36bd5809b5"
                        "5ea6ed2f080df4000697c90c"
                        "5ea6ed2e080df4000697c909"
                        "5ea6ed2f080df4000697c90b"
                    ],
                ],
                "capsules": [[], ["5fbb0f8fec55b34eb9f35c14"]],
                "payloads": [
                    ["5eb0e4d3b6c3bb0006eeb262"],
                    [
                        "605b4bfcaa5433645e37d048"
                        "609f48374a12e4692eae4667"
                        "609f49c64a12e4692eae4668"
                    ],
                ],
                "launchpad": ["5e9e4502f509092b78566f87", "5e9e4501f509094ba4566f84"],
                "flight_number": [1, 2],
                "name": [
                    "Failure Case",
                    "Success Case",
                ],
                "date_utc": [
                    "2020-11-25T02:13:00.000Z",
                    "2020-11-25T02:13:00.000Z",
                ],
                "date_local": [
                    "2020-11-24T21:13:00-05:00",
                    "2020-11-24T21:13:00-05:00",
                ],
                "date_precision": "hour",
                "upcoming": False,
                "auto_update": True,
                "tbd": False,
                "launch_library_id": None,
            }
        )
        for col in [
            "recovered",
            "recovery_attempt",
            "reused",
            "net",
            "upcoming",
            "auto_update",
            "tbd",
        ]:
            data[col] = data[col].astype("boolean")
        for col in ["window", "success"]:
            data[col] = data[col].astype("Int64")
        FairingsSchema.to_schema().validate(data)
        return data

    @freeze_time(FROZEN_CREATE_DATE)
    def test_spacex_fairings_creates_fairings_data(
        self, bronzified_data, fairings_data, tmp_path
    ):
        with patch(
            "spacex_data_platform.ingestion.silver.cores_data.pd.read_parquet"
        ) as read_parquet_mock:
            read_parquet_mock.return_value = bronzified_data
            expected_path = f"{tmp_path}/silver/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/fairings.parquet"
            result_path = SpaceXFairings.run(
                f"{tmp_path}/bronze/{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}/spacex_data.parquet"
            )
        assert expected_path == result_path

        result = pd.read_parquet(result_path)
        pd.testing.assert_frame_equal(result, fairings_data)
