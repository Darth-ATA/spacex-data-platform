import pandas as pd
import pandera as pa
from pandera.typing import Series


class CoresFlightsSchema(pa.DataFrameModel):
    id: Series[str] = pa.Field(nullable=True, coerce=True)
    core: Series[str] = pa.Field(nullable=False, coerce=True)
    flight: Series[int] = pa.Field(nullable=False, coerce=True)
    gridfins: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    landing_attempt: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    landing_success: Series[pd.BooleanDtype] = pa.Field(nullable=True, coerce=True)
    landing_type: Series[str] = pa.Field(nullable=True, coerce=True)
    landpad: Series[str] = pa.Field(nullable=True, coerce=True)
    legs: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)
    reused: Series[pd.BooleanDtype] = pa.Field(nullable=False, coerce=True)

    @pa.dataframe_check
    def primary_key(self, df: pd.DataFrame) -> Series[bool]:
        """Check if the combination of 'id' and 'core' is unique

        Args:
            df (pd.DataFrame): dataframe to be checked

        Returns:
            Series[bool]: returns what rows accomplish and what not the condition
        """
        return ~df.duplicated(subset=["id", "core"])

    @pa.dataframe_check
    def if_landing_success_landpad_and_landing_type_are_filled(
        self,
        df: pd.DataFrame,
    ) -> Series[bool]:
        """If landing_attempt is True, landing_success, landpad and landing_type must be filled
        Except if landing_type is 'Ocean' (We need to check why are we considering this a success)

        Args:
            df (pd.DataFrame): dataframe to be checked

        Returns:
            Series[bool]: returns what rows acomplishes and what not the condition
        """
        column_applicable = df[
            (df["landing_success"]) & (df["landing_type"] != "Ocean")
        ][["landing_type", "landpad"]]
        return (
            column_applicable["landing_type"].notna()
            & column_applicable["landpad"].notna()
        )

    @pa.dataframe_check
    def if_core_is_reused_flight_bigger_than_one(
        self,
        df: pd.DataFrame,
    ) -> Series[bool]:
        """If core is reused, flight must be bigger than 1

        Args:
            df (pd.DataFrame): dataframe to be checked

        Returns:
            Series[bool]: returns what rows acomplishes and what not the condition
        """
        column_applicable = df[df["reused"]]["flight"]
        return column_applicable > 1
