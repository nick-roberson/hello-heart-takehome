from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, List

import boto3
import pandas as pd
from constants import (BUCKET, ENDPOINT_URL, PARQUET_FILE_FAILED_INSERTS,
                       PARQUET_FILE_SUCCEEDED_INSERTS, TRANSFORM_CHUNKSIZE)

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


def get_localstack_dynamodb_resource():
    """Get localstack DynamoDB client"""
    resource = boto3.resource(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    return resource


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


def _split_df(
    covid_df: pd.DataFrame, chunk_size: int = TRANSFORM_CHUNKSIZE
) -> List[pd.DataFrame]:
    """Split task helper function. Split the DataFrame into chunks of 10000.

    Args:
        covid_df (pd.DataFrame): DataFrame to split
        chunk_size (int): Size of each chunk
    Returns:
        List[pd.DataFrame]: List of DataFrames
    """
    # Split the dataframe into chunks of 10000
    chunks = []
    for i, chunk in enumerate(covid_df.groupby(covid_df.index // chunk_size)):
        chunks.append(chunk[1])
    # Return the chunks
    return chunks


def _combine_df(covid_dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Combine task helper function. Combine the DataFrames into a single DataFrame.

    Args:
        covid_dfs (List[pd.DataFrame]): List of DataFrames to combine
    Returns:
        pd.DataFrame: Combined DataFrame
    """
    # combine the dataframes
    return pd.concat(covid_dfs)


def _transform_df(covid_df: pd.DataFrame) -> pd.DataFrame:
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

    # Parsing dates to datetime and then back to string
    covid_df["Last Report Date"] = covid_df["Last Report Date"].apply(parse_date)
    covid_df["Last Report Date"] = covid_df["Last Report Date"].dt.strftime("%Y-%m-%d")

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

    # Convert float columns to string (to prepare for DynamoDB)
    float_cols = covid_df.select_dtypes(include=["float"]).columns
    covid_df[float_cols] = covid_df[float_cols].astype(str)

    # --------- Data Cleaning ---------

    # Rename columns to snake case and lowercase for consistency
    def clean_col_name(col: str) -> str:
        """Simple clean the column name of any non-standard characters."""
        cleaned_col = col.lower().replace(" ", "_")
        cleaned_col = "".join(e for e in cleaned_col if e.isalnum() or e == "_")
        return cleaned_col

    column_mapping = {col: clean_col_name(col) for col in covid_df.columns}
    covid_df = covid_df.rename(columns=column_mapping)
    return covid_df


def process_failed_inserts(
    client, failed_inserts: List[Dict], etl_run_id: str, result: Dict
):
    """Process failed inserts.

    Args:
        client: S3 client
        failed_inserts: List of failed inserts
        etl_run_id: ETL run ID
        result: Result dictionary
    Returns:
        Dict: Updated result dictionary
    """
    logger.error(f"Failed to insert {len(failed_inserts)} models")

    # Push the failed inserts to S3 as a parquet file
    with tempfile.TemporaryDirectory() as tmpdir:
        failed_inserts_df = pd.DataFrame(failed_inserts)
        failed_inserts_path = os.path.join(tmpdir, PARQUET_FILE_FAILED_INSERTS)
        failed_inserts_df.to_parquet(failed_inserts_path)
        failed_inserts_key = f"{etl_run_id}/{PARQUET_FILE_FAILED_INSERTS}"
        s3_upload(client, failed_inserts_path, BUCKET, failed_inserts_key)

        # Update the result with the S3 path and count
        result["failed_inserts_path"] = f"s3://{BUCKET}/{failed_inserts_key}"
        result["failed_inserts_count"] = len(failed_inserts)
        logger.info(
            f"Uploaded failed inserts to S3: bucket={BUCKET}, key={failed_inserts_key}"
        )
    return result


def process_suceeded_inserts(
    client, succeeded_inserts: List[Dict], etl_run_id: str, result: Dict
):
    """Process succeeded inserts.

    Args:
        client: S3 client
        succeeded_inserts: List of succeeded inserts
        etl_run_id: ETL run ID
        result: Result dictionary
    Returns:
        Dict: Updated result dictionary
    """
    logger.info(f"Successfully inserted {len(succeeded_inserts)} models")

    # Push the succeeded inserts to S3 as a parquet file
    with tempfile.TemporaryDirectory() as tmpdir:
        succeeded_inserts_df = pd.DataFrame(succeeded_inserts)
        succeeded_inserts_path = os.path.join(tmpdir, PARQUET_FILE_SUCCEEDED_INSERTS)
        succeeded_inserts_df.to_parquet(succeeded_inserts_path)
        succeeded_inserts_key = f"{etl_run_id}/{PARQUET_FILE_SUCCEEDED_INSERTS}"
        s3_upload(client, succeeded_inserts_path, BUCKET, succeeded_inserts_key)

        # Update the result with the S3 path and count
        result["succeeded_inserts_path"] = f"s3://{BUCKET}/{succeeded_inserts_key}"
        result["succeeded_inserts_count"] = len(succeeded_inserts)
        logger.info(
            f"Uploaded succeeded inserts to S3: bucket={BUCKET}, key={succeeded_inserts_key}"
        )
    return result
