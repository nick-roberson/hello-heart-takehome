# Hello Heart Take Home

Airflow ETL Assignment for Hello Heart. This project is a simple ETL pipeline that reads data from a CSV file, processes it using Airflow, and writes it to a MySQL database.

## LocalStack

I setup a trial account on localstack, so I will use that for all the AWS Services that I will use 
in order to complete this take home.

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