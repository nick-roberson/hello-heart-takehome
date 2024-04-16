# Path to the CSV file
DATA_CSV = "dags/data/COVID-19_Public_Therapeutic_Locator_20240412.csv"

# Transform Constants
TRANSFORM_CHUNKSIZE = 10000
INSERT_CHUNKSIZE = 1000

# Localstack S3 endpoint
ENDPOINT_URL = "http://host.docker.internal:4566"

BUCKET = "hello-heart-covid-data"

# S3 file names and paths
PARQUET_FILE_RAW = "covid_data_raw.parquet"
PARQUET_FILE_PROCESSED = "covid_data_processed.parquet"
PARQUET_FILE_FAILED_INSERTS = "failed_inserts.parquet"
PARQUET_FILE_SUCCEEDED_INSERTS = "succeeded_inserts.parquet"
