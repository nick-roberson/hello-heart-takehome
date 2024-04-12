from __future__ import annotations

import logging
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import Dict

import boto3
import pandas as pd
import pendulum
from airflow.decorators import dag, task

# Path to the CSV file
DATA_CSV = "dags/data/COVID-19_Public_Therapeutic_Locator_20240412.csv"

# Transform Constants
TRANSFORM_CHUNKSIZE = 10000

# Localstack S3 endpoint
# ENDPOINT_URL = "http://localhost.localstack.cloud:4566"
# ENDPOINT_URL = "https://0.0.0.0:4566"
ENDPOINT_URL = "http://host.docker.internal:4566"

BUCKET = "hello-heart-covid-data"

# S3 file names and paths
PARQUET_FILE_RAW = "covid_data_raw.parquet"
PARQUET_FILE_PROCESSED = "covid_data_processed.parquet"

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


def get_localstack_s3_client():
    """Get localstack S3 client"""
    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    return client


def s3_exists(client, bucket: str, key: str):
    """Check if a file exists in S3"""
    try:
        client.head_object(Bucket=bucket, Key=key)
        logging.info(f"File {key} exists in S3")
        return True
    except BaseException as e:
        logging.info(f"File {key} does not exist in S3, error: {e}")
        return False


def s3_upload(client, file_path: str, bucket: str, key: str):
    """Upload a file to S3"""
    try:
        client.upload_file(file_path, bucket, key)
        logging.info(f"Uploaded {file_path} to s3://{bucket}/{key}")
    except BaseException as e:
        logging.error(f"Failed to upload {file_path} to S3, error: {e}")
        raise e


def s3_download(client, bucket: str, key: str, file_path: str):
    """Download a file from S3"""
    try:
        client.download_file(bucket, key, file_path)
        logging.info(f"Downloaded s3://{bucket}/{key} to {file_path}")
    except BaseException as e:
        logging.error(
            f"Failed to download s3://{bucket}/{key} to {file_path}, error: {e}"
        )
        raise e


def split(covid_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Split task helper function. Split the DataFrame into chunks of 10000.

    Args:
        covid_df (pd.DataFrame): DataFrame to split
    Returns:
        Dict[str, pd.DataFrame]: Split DataFrames
    """
    # Split the dataframe into chunks of 10000
    chunks = {}
    for i, chunk in enumerate(covid_df.groupby(covid_df.index // TRANSFORM_CHUNKSIZE)):
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
        pd.DataFrame: Processed DataFrame
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
    column_mapping = {col: col.lower().replace(" ", "_") for col in covid_df.columns}
    covid_df = covid_df.rename(columns=column_mapping)
    return covid_df


#################################################################
# Airflow                                                       #
#################################################################


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["covid19", "etl"],
)
def covid_etl(run_id: str = str(uuid.uuid4())):
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

    Processed format:
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

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract(etl_run_id: str, data_path: str) -> str:
        """Extract task. Load data from CSV file into a DataFrame. Does some basic data cleaning and validation.

        Args:
            etl_run_id (str): Unique run ID
            data_path (str): Path to the CSV file
        Returns:
            str: Path to the raw parquet file uploaded to S3
        """
        raw_s3_key = f"{etl_run_id}/{PARQUET_FILE_RAW}"
        s3_path = f"s3://{BUCKET}/{raw_s3_key}"

        # Check contents of file
        if not os.path.exists(data_path):
            raise ValueError(f"File does not exist at path {data_path}")

        # If the file is already uploaded, skip the extraction, just return the path
        client = get_localstack_s3_client()
        if s3_exists(client, BUCKET, raw_s3_key):
            return s3_path

        with tempfile.TemporaryDirectory("covid_etl") as tmpdir:
            # Load data into a dataframe, deduplicate, and drop empty rows
            df = pd.read_csv(data_path)
            df = df.drop_duplicates()
            df = df.dropna(how="all")

            # Show some basic info about the dataframe
            log_df_info(df)

            # Save the dataframe to a parquet file
            parquet_path = os.path.join(tmpdir, PARQUET_FILE_RAW)
            df.to_parquet(parquet_path)
            s3_upload(client, parquet_path, BUCKET, raw_s3_key)

        # Return the S3 path
        return raw_s3_key

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def transform(etl_run_id: str, raw_s3_key: str) -> str:
        """Transform task. Split the DataFrame into chunks, transform each chunk, and combine them back.

        Args:
            etl_run_id (str): Unique run ID
            raw_s3_path (str): Path to the raw parquet file uploaded to S3
        Returns:
            str: Path to the processed parquet file uploaded to S3
        """
        # S3 paths for output files
        raw_s3_path = f"s3://{BUCKET}/{raw_s3_key}"
        processed_s3_key = f"{etl_run_id}/{PARQUET_FILE_PROCESSED}"
        processed_s3_path = f"s3://{BUCKET}/{processed_s3_key}"

        # Check if processed file is already uploaded
        client = get_localstack_s3_client()
        if s3_exists(client, BUCKET, processed_s3_key):
            logging.info(f"Processed file already exists at {processed_s3_path}")
            return processed_s3_path

        # Check if the raw file is present in S3
        if not s3_exists(client, BUCKET, raw_s3_key):
            raise ValueError(f"File does not exist at path {raw_s3_path}")

        with tempfile.TemporaryDirectory("covid_etl") as tmpdir:
            # Download the raw data from S3
            raw_local_path = os.path.join(tmpdir, PARQUET_FILE_RAW)
            s3_download(client, BUCKET, raw_s3_key, raw_local_path)

            # Split, transform, and combine the data
            covid_df = pd.read_parquet(raw_local_path)
            processed_dfs = {k: _transform(v) for k, v in split(covid_df).items()}
            covid_df = combine(processed_dfs)

            # Push the processed dataframe back to S3
            processed_local_path = os.path.join(tmpdir, PARQUET_FILE_PROCESSED)
            covid_df.to_parquet(processed_local_path)
            s3_upload(client, processed_local_path, BUCKET, processed_s3_key)

        # Return the S3 path
        return processed_s3_key

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def load(etl_run_id: str, processed_s3_key: str):
        """Load task. Load the processed DataFrame into a database.

        Args:
            etl_run_id (str): Unique run ID
            processed_s3_key (str): Path to the processed parquet file uploaded to S3
        """
        # S3 paths for output files
        processed_s3_path = f"s3://{BUCKET}/{processed_s3_key}"

        # Confirm that the processed data is present in S3
        client = get_localstack_s3_client()
        if not s3_exists(client, BUCKET, processed_s3_key):
            raise ValueError(f"File does not exist at path {processed_s3_path}")

        with tempfile.TemporaryDirectory("covid_etl") as tmpdir:
            # Download the processed data from S3
            processed_local_path = os.path.join(tmpdir, PARQUET_FILE_PROCESSED)
            s3_download(client, BUCKET, processed_s3_key, processed_local_path)

            # Load the processed data into a dataframe
            covid_df = pd.read_parquet(processed_local_path)

            # Show some basic info about the dataframe
            log_df_info(covid_df)

    # 1. Load data from file (S3 used as cache)
    raw_s3_key = extract(run_id, DATA_CSV)

    # 2. Transform the data (S3 used as cache)
    processed_s3_key = transform(run_id, raw_s3_key)

    # 3. Load the data into a database
    load(run_id, processed_s3_key)


dag = covid_etl()
