from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd
import pendulum
from airflow.decorators import dag, task

# Path to the CSV file
DATA_CSV = "dags/data/COVID-19_Public_Therapeutic_Locator_20240412.csv"

# S3 Paths for data (each run will be under its own run_id)
S3_BUCKET = "s3://covid19-data"

# Transform Constants
TRANSFORM_CHUNKSIZE = 10000


#################################################################
# Logging                                                       #
#################################################################
def setup_logging():
    """Init some basic logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_logger():
    """Get logger"""
    setup_logging()
    return logging.getLogger(__name__)


logger = get_logger()


#################################################################
# Utils                                                         #
#################################################################


def parse_date(date_str: str) -> datetime:
    """Parse date string"""
    # Different date formats to try
    formats_to_try = ["%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S %p"]

    # Try parsing the date string with each format
    for format_str in formats_to_try:
        try:
            # Break out of loop once parsed successfully
            return datetime.strptime(date_str, format_str)
        except ValueError:
            # If parsing fails for a format, try the next one
            pass


def log_df_info(df: pd.DataFrame):
    """Log dataframe info"""
    # Show some basic info about the dataframe
    logger.info("DataFrame Info:")
    logger.info(f"{df.info()}")
    logger.info("DataFrame Head:")
    logger.info(f"{df.head(10)}")

    # Show info about rows and columns
    logger.info("Rows Count: %s", df.shape[0])
    logger.info("Columns Count: %s", df.shape[1])
    logger.info("Columns: %s", df.columns.tolist())


#################################################################
# Airflow                                                       #
#################################################################


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["covid19", "etl"],
)
def covid_etl(file_path: str = DATA_CSV):
    """Transforms COVID-19 data from a CSV file and loads it into a database

    Original format:
        Data columns (total 15 columns):
         #   Column              Non-Null Count  Dtype
        ---  ------              --------------  -----
         0   Provider Name       69184 non-null  object
         1   Address1            69184 non-null  object
         2   Address2            24603 non-null  object
         3   City                69184 non-null  object
         4   County              69184 non-null  object
         5   State Code          69184 non-null  object
         6   Zip                 69184 non-null  int64
         7   National Drug Code  69175 non-null  object
         8   Order Label         69184 non-null  object
         9   Courses Available   69111 non-null  float64
         10  Geocoded Address    69182 non-null  object
         11  NPI                 67494 non-null  float64
         12  Last Report Date    69184 non-null  object
         13  Provider Status     69184 non-null  object
         14  Provider Note       38760 non-null  object

    Transformed format:
        Data columns (total 21 columns):
         #   Column              Non-Null Count  Dtype
        ---  ------              --------------  -----
         0   provider_name       69184 non-null  object        <-- Original columns
         1   address1            69184 non-null  object
         2   address2            24603 non-null  object
         3   city                69184 non-null  object
         4   county              69184 non-null  object
         5   state_code          69184 non-null  object
         6   zip                 69184 non-null  int64
         7   national_drug_code  69175 non-null  object
         8   order_label         69184 non-null  object
         9   courses_available   69111 non-null  float64
         10  geocoded_address    69182 non-null  object
         11  npi                 67494 non-null  float64
         12  last_report_date    69184 non-null  datetime64[ns]
         13  provider_status     69184 non-null  object
         14  provider_note       38760 non-null  object
         15  street              69184 non-null  object        <-- Start of new columns
         16  state               69184 non-null  object
         17  zip_code            69184 non-null  int64
         18  provider_type       69184 non-null  object
         19  latitude            69182 non-null  object
         20  longitude           69182 non-null  object
    """

    @task()
    def extract(data_path: str) -> pd.DataFrame:
        """Extract task. Load data from CSV file into a DataFrame. Does some basic data cleaning and validation.

        Args:
            data_path (str): Path to the CSV file
        Returns:
            pd.DataFrame: Extracted DataFrame
        """
        # Check contents of file
        if not os.path.exists(data_path):
            raise ValueError(f"File does not exist at path {data_path}")

        # Load data into a dataframe, deduplicate, and drop empty rows
        df = pd.read_csv(data_path)
        df = df.drop_duplicates()
        df = df.dropna(how="all")

        # Show some basic info about the dataframe
        log_df_info(df)

        return df

    def split(covid_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Split task helper function. Split the DataFrame into chunks of 10000.

        Args:
            covid_df (pd.DataFrame): DataFrame to split
        Returns:
            Dict[str, pd.DataFrame]: Split DataFrames
        """
        # Split the dataframe into chunks of 10000
        chunks = {}
        for i, chunk in enumerate(
            covid_df.groupby(covid_df.index // TRANSFORM_CHUNKSIZE)
        ):
            chunks[f"chunk_{i}"] = chunk[1]

        # Return the chunks
        return chunks

    def combine(covid_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combine task helper function. Combine the DataFrames into a single DataFrame.

        Args:
            covid_dfs (Dict[str, pd.DataFrame]): DataFrames to combine
        Returns:
            pd.DataFrame: Combined DataFrame
        """
        # combine the dataframes
        return pd.concat(covid_dfs.values())

    def _transform(covid_df: pd.DataFrame) -> pd.DataFrame:
        """Transform task helper function. Transform the DataFrame.

        Args:
            covid_df (pd.DataFrame): DataFrame to transform
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # check if data is present
        if covid_df.empty:
            raise ValueError("No data present")

        # --------- Feature Tuning  ---------

        # Extract relevant information from addresses
        covid_df["Street"] = covid_df["Address1"].str.split(",").str[0]
        covid_df["City"] = covid_df["City"]
        covid_df["State"] = covid_df["State Code"]
        covid_df["Zip Code"] = covid_df["Zip"]

        # Parsing dates
        covid_df["Last Report Date"] = covid_df["Last Report Date"].apply(parse_date)

        # Extract provider type
        covid_df["Provider Type"] = covid_df["Provider Name"].str.split(" ").str[0]

        # --------- Data Enrichment ---------

        # Geocoding (assuming Geocoded Address is in WKT format)
        covid_df["Latitude"] = covid_df["Geocoded Address"].apply(
            lambda x: x.split(" ")[1][1:] if x else None
        )
        covid_df["Longitude"] = covid_df["Geocoded Address"].apply(
            lambda x: x.split(" ")[2][:-1] if x else None
        )

        # --------- Data Validation ---------

        # Check for missing values and log, but don't raise an error
        missing_values = covid_df.isnull().sum()
        if missing_values.any():
            logging.warning(
                f"Missing values found in columns: {missing_values[missing_values > 0]}"
            )

        # --------- Data Cleaning ---------

        # Rename columns to snake case for DB compatibility
        column_mapping = {
            col: col.lower().replace(" ", "_") for col in covid_df.columns
        }
        covid_df = covid_df.rename(columns=column_mapping)

        return covid_df

    @task()
    def transform(covid_df: pd.DataFrame) -> pd.DataFrame:
        """Transform task. Split the DataFrame into chunks, transform each chunk, and combine them back.

        Args:
            covid_df (pd.DataFrame): DataFrame to transform
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        # check if data is present
        if covid_df.empty:
            raise ValueError("No data present")

        # split and process
        split_dfs = split(covid_df)
        transformed_dfs = {k: _transform(v) for k, v in split_dfs.items()}
        return combine(transformed_dfs)

    @task()
    def load(covid_df: pd.DataFrame):
        """Load task. Load the transformed DataFrame into a database.

        Args:
            covid_df (pd.DataFrame): Transformed DataFrame
        """
        # Show some basic info about the dataframe
        log_df_info(covid_df)

    # Load data
    covid_df: pd.DataFrame = extract(data_path=file_path)

    # Transform
    combined_df: pd.DataFrame = transform(covid_df)

    # Load
    load(combined_df)


dag = covid_etl()
