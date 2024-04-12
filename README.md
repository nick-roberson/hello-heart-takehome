# Hello Heart Take Home

Airflow ETL Assignment for Hello Heart. This project is a simple ETL pipeline that reads data from a CSV file, processes it using Airflow, and writes it to a MySQL database.

## LocalStack

I set up a trial account on localstack, so I will use that for all the AWS Services that I will use 
in order to complete this take home.

## Create S3 Bucket

I will create a bucket in S3 to store the data from runs of the ETL pipeline. This will be used to store the raw data as well as the processed data.
```bash
awslocal s3api create-bucket --bucket hello-heart-covid-data
```
Confirm that the bucket was created by running:
```bash
awslocal s3api list-buckets | grep hello-heart-covid-data
```

For some reason I cant seem to use the `boto3` client with the localstack, so I will use the `awslocal` command line tool to interact with the S3 bucket in all the code. I think it requires a paid version but I am just running using the free tier.

## Create SQL Database

I was originally planning to use RDS, but it does not look like the localstack supports RDS on their free tier.
So I will use a connection to my own local MySQL database. I will create a database called `hello_heart` and a table called `covid_data` to store the data from the ETL pipeline.


```bash

## Setup Airflow Server

All steps to start up the Airflow server are in the `setup.sh` bash file. Run it like so:
```bash
bash setup.sh
```

This should set up the database as well as the webserver and scheduler. The webserver should be running on `localhost:8080`.

Once this is done you should be able to see the `CovidETL` DAG in the Airflow UI.

## Running the ETL

To run the ETL, simply trigger the `CovidETL` DAG in the Airflow UI. This will run the ETL pipeline and write the data to the MySQL database.

## Docs Source

- https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html