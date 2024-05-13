import os
from urllib.error import URLError

import duckdb
import pandas as pd
import streamlit as st

from spacex_data_platform.__main__ import Main

spacex_ingestion_main = Main()


def get_all_create_date() -> list:
    """Get all the create dates available

    Returns:
        list: A list with all the create dates available
    """
    create_dates = os.listdir("data/silver")
    return sorted(create_dates, reverse=True)


def get_latest_create_date(create_dates: list[str]) -> str:
    """Get the latest create date available

    Returns:
        list: latest create date available
    """
    return create_dates[0]


def get_data(location: str) -> pd.DataFrame:
    """Read the data from the parquet file in the location

    Args:
        location (str): The location of the parquet file

    Returns:
        pd.DataFrame: The data in the parquet file
    """
    df = pd.read_parquet(location)
    df = df.apply(
        lambda col: pd.to_datetime(col, errors="ignore")
        if col.dtypes == object
        else col,
        axis=0,
    )
    return df


def run_ingestion():
    spacex_ingestion_main.run()


def get_max_number_of_times_a_core_has_been_used() -> pd.DataFrame:
    query = """
SELECT core, MAX(flight) AS max_number_of_times_used
    FROM cores_df
GROUP BY core
ORDER BY MAX(flight) DESC LIMIT 1
    """
    st.markdown(f"```{query}")
    result = duckdb.sql(query).df()
    return result


def get_cores_used_in_less_than_x_days(number_of_days: int) -> pd.DataFrame:
    query = f"""
SELECT fai.id, cor.core, fai.date_utc
    FROM cores_df as cor
INNER JOIN fairings_df AS fai
    ON cor.id = fai.id
WHERE date_diff('day', fai.date_utc, current_timestamp) < {number_of_days}
    """
    st.markdown(f"```{query}")
    result = duckdb.sql(query).df()
    return result


def get_months_in_which_there_has_been_more_than_one_launch(
    number_of_launches: int,
) -> pd.DataFrame:
    query = f"""
SELECT date_trunc('month', date_utc) AS month, COUNT(*) AS number_of_launches
    FROM fairings_df
GROUP BY month
    HAVING COUNT(*) > {number_of_launches}
    """
    st.markdown(f"```{query}")
    result = duckdb.sql(query).df()
    return result


st.title("SpaceX Explorer")
if st.button("Run Ingestion", type="primary"):
    run_ingestion()
try:
    create_dates = [""] + get_all_create_date()
    if len(create_dates) == 1:
        st.error("No data available, click the button above to run the ingestion")
    else:
        st.info(f"Latest data available {create_dates[1]}", icon="ℹ️")
        selected_create_date = st.selectbox("Choose data version", create_dates)

        if selected_create_date == "":
            st.info("Select a data version to explore")
        else:
            fairings_df = get_data(
                f"data/silver/{selected_create_date}/fairings.parquet"
            )
            cores_df = get_data(
                f"data/silver/{selected_create_date}/cores_flight.parquet"
            )

            # available_fairings = fairings_df.index.get_level_values(
            #    "id"
            # ).drop_duplicates()
            # available_cores = cores_df.index.get_level_values(
            #    "core"
            # ).drop_duplicates()
            #
            # fairing_id = st.multiselect("Choose fairing_id's", available_fairings)
            # core_id = st.multiselect("Choose core", available_cores)

            st.markdown(f"## {selected_create_date} Data")
            st.markdown("### Fairings Data")
            st.dataframe(fairings_df)
            st.markdown("### Cores Data")
            st.dataframe(cores_df)
            st.markdown("### Assesement questions")
            st.markdown(
                "- Each time a rocket is launched, one or more cores (first stages) are involved. Sometimes, cores are recovered after the launch and reused posteriorly in another launch. What is the maximum number of times a core has been used? Write an SQL query to find the result."
            )
            st.dataframe(get_max_number_of_times_a_core_has_been_used())
            st.markdown(
                "- Which cores have been reused in less than 1000 days after the previous launch? Write an SQL query to find the result."
            )
            st.dataframe(get_cores_used_in_less_than_x_days(1000))
            st.markdown(
                "- List the months in which there has been more than one launch. Write an SQL query to find the results."
            )
            st.dataframe(get_months_in_which_there_has_been_more_than_one_launch(1))

except URLError as e:
    st.error(
        """
        **This demo requires internet access.**
        Connection error: %s
    """
        % e.reason
    )
