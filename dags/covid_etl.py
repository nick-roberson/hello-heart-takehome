from __future__ import annotations

import logging
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Dict

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from constants import (BUCKET, DATA_CSV, INSERT_CHUNKSIZE,
                       PARQUET_FILE_PROCESSED, PARQUET_FILE_RAW)
from db.models import Provider
from utils import (_combine_df, _split_df, _transform_df,
                   get_localstack_dynamodb_resource, get_localstack_s3_client,
                   log_df_info, process_failed_inserts,
                   process_suceeded_inserts, s3_download, s3_exists, s3_upload)

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
# Airflow                                                       #
#################################################################


@dag(
    # Run once a day, this data may not be updated frequently (according to the source)
    schedule="0 0 * * *",
    # Start the date after I complete this
    start_date=pendulum.datetime(2024, 4, 15, tz="UTC"),
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
    """

    @task(retries=3, retry_delay=timedelta(seconds=10))
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

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def transform(etl_run_id: str, raw_s3_key: str) -> str:
        """Transform task. Split the DataFrame into chunks, transform each chunk, and combine them back.

        Notes:
            - This task could be parallelized in a better way for larger datasets. Here I'm just using ThreadPoolExecutor
            to process the chunks in parallel.

        Args:
            etl_run_id (str): Unique run ID
            raw_s3_key (str): Path to the raw parquet file uploaded to S3
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

            # Split the data into chunks, transform, and combine
            split_dfs = _split_df(covid_df)

            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = executor.map(_transform_df, split_dfs)
                transformed_chunks = list(results)

            # Combine the transformed dataframes
            covid_df = _combine_df(transformed_chunks)

            # Push the processed dataframe back to S3
            processed_local_path = os.path.join(tmpdir, PARQUET_FILE_PROCESSED)
            covid_df.to_parquet(processed_local_path)
            s3_upload(client, processed_local_path, BUCKET, processed_s3_key)

        # Return the S3 path
        return processed_s3_key

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def load(etl_run_id: str, processed_s3_key: str) -> Dict:
        """Load task. Load the processed DataFrame into a database.

        Notes:
            - Here I originally wanted to use RDS however I had to switch to DynamoDB due to the limitations of
                Localstack free tier.
            - Additionally, I would have liked to do this in parallel, however I was unable to get my standalone
                Airflow instance to use the correct parallel runner for some reason.

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

        # Download the processed data from S3 to temp directory
        with tempfile.TemporaryDirectory("covid_etl") as tmpdir:
            # Download the processed data from S3
            processed_local_path = os.path.join(tmpdir, PARQUET_FILE_PROCESSED)
            s3_download(client, BUCKET, processed_s3_key, processed_local_path)
            # Load the processed data into a dataframe
            covid_df = pd.read_parquet(processed_local_path)
            log_df_info(covid_df)

        # Load the data into the DynamoDB table
        dynamo_client = get_localstack_dynamodb_resource()
        providers_table = dynamo_client.Table("providers")

        failed_inserts, succeeded_inserts = [], []
        chunks = _split_df(covid_df, chunk_size=INSERT_CHUNKSIZE)
        total_chunks = len(chunks)

        # Insert the data into the DynamoDB table
        for index, chunk in enumerate(chunks):
            logging.info(
                f"Starting chunk {index}/{total_chunks} into DynamoDB (size {INSERT_CHUNKSIZE})"
            )
            start_time = pendulum.now()

            # Process the chunk and insert into DynamoDB
            with providers_table.batch_writer() as batch:
                insert_models = [
                    Provider(**row) for row in chunk.to_dict(orient="records")
                ]
                for model in insert_models:
                    model_dict = model.dict()
                    try:
                        batch.put_item(Item=model_dict)
                        succeeded_inserts.append(model_dict)
                    except Exception as e:
                        failed_inserts.append(model_dict)
                        logger.error(
                            f"Failed to insert model: {model_dict}, error: {e}"
                        )
                        pass

            duration = (pendulum.now() - start_time).total_seconds()
            logging.info(f"Chunk {index}/{total_chunks} inserted in {duration}s")

        result = {}

        # Log failed inserts and historize them in S3
        if failed_inserts:
            process_failed_inserts(client, failed_inserts, etl_run_id, result)

        # Log successful inserts and historize them in S3
        if succeeded_inserts:
            process_suceeded_inserts(client, succeeded_inserts, etl_run_id, result)

        return result

    # 1. Load data from file (S3 used as cache)
    raw_s3_key = extract(run_id, DATA_CSV)

    # 2. Transform the data (S3 used as cache)
    processed_s3_key = transform(run_id, raw_s3_key)

    # 3. Load the data into a database
    load(run_id, processed_s3_key)


dag = covid_etl()
