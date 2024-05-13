"""This module contains the schema to validate the fairings data."""

import pandas as pd
import pandera as pa
from pandera.typing import Series


class FairingsSchema(pa.DataFrameModel):
    """Schema to validate the fairings data"""

    id: Series[str] = pa.Field(nullable=True, coerce=True, unique=True)
    provider_code: Series[str] = pa.Field(nullable=False, coerce=True)
    create_date: Series[str] = pa.Field(nullable=False, coerce=True)
    recovered: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    recovery_attempt: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    reused: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    recovery_ships: Series[list[str]] = pa.Field(nullable=True, coerce=True)
    static_fire_date_utc: Series[str] = pa.Field(nullable=True, coerce=True)
    net: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    window: Series[pd.Int64Dtype] = pa.Field(nullable=True, coerce=True)
    rocket: Series[str] = pa.Field(nullable=False, coerce=True)
    success: Series[pd.Int64Dtype] = pa.Field(nullable=True, coerce=True, ge=0, le=1)
    details: Series[str] = pa.Field(nullable=True, coerce=True)
    ships: Series[list[str]] = pa.Field(nullable=False, coerce=True)
    capsules: Series[list[str]] = pa.Field(nullable=False, coerce=True)
    payloads: Series[list[str]] = pa.Field(nullable=False, coerce=True)
    launchpad: Series[str] = pa.Field(nullable=False, coerce=True)
    flight_number: Series[int] = pa.Field(nullable=False, coerce=True)
    name: Series[str] = pa.Field(nullable=False, coerce=True)
    date_utc: Series[str] = pa.Field(
        nullable=False,
        coerce=True,
    )
    date_local: Series[str] = pa.Field(
        nullable=False,
        coerce=True,
    )
    date_precision: Series[str] = pa.Field(nullable=False, coerce=True)
    upcoming: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    auto_update: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    tbd: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    launch_library_id: Series[str] = pa.Field(nullable=True, coerce=True)

    @pa.check(create_date, name="create_date_format")
    def create_date_format(cls, date_str: Series[str]) -> Series[bool]:
        """Check if the date has the format 'YYYY-MM-DDTHH:MM:SS'

        Args:
            date_str (Series[str]): date to be checked

        Returns:
            Series[bool]: returns what rows accomplish and what not the condition
        """
        return date_str.str.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")

    @pa.check(static_fire_date_utc, name="static_fire_date_utc_format")
    def static_fire_date_utc_format(cls, date_str: Series[str]) -> Series[bool]:
        """Check if the date has the format 'YYYY-MM-DDTHH:MM:SS'

        Args:
            date_str (Series[str]): date to be checked

        Returns:
            Series[bool]: returns what rows accomplish and what not the condition
        """
        return date_str.isna() | date_str.str.match(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
        )

    @pa.check(date_utc, name="date_utc_format")
    def date_utc_format(cls, date_str: Series[str]) -> Series[bool]:
        """Check if the date has the format 'YYYY-MM-DDTHH:MM:SS.mmmZ'

        Args:
            date_str (Series[str]): date to be checked

        Returns:
            Series[bool]: returns what rows accomplish and what not the condition
        """
        return date_str.str.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z")

    @pa.check(date_local, name="date_local_format")
    def date_local_format(cls, date_str: Series[str]) -> Series[bool]:
        """Check if the date has the format 'YYYY-MM-DDTHH:MM:SS+-HH:MM'

        Args:
            date_str (Series[str]): date to be checked

        Returns:
            Series[bool]: returns what rows accomplish and what not the condition
        """
        return date_str.str.match(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\+,-]\d{2}:\d{2}"
        )
