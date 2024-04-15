# Hello Heart Take Home

Airflow ETL Assignment for Hello Heart. This project is a simple ETL pipeline that reads data from a CSV file, processes it using Airflow, and writes it to a MySQL database.

## Airflow 

To startup the Airflow server, you can use the following command:
```bash
bash setup.py
```
This will start the Airflow server and the webserver. You can access the Airflow UI at `http://localhost:8080`.

## LocalStack

I set up a trial account on localstack, so I will use that for all the AWS Services that I will use 
in order to complete this take home.

## Create S3 `hello-heart-covid-data` Bucket
I will create a bucket in S3 to store the data from runs of the ETL pipeline. This will be used to store the raw data as well as the processed data.
```bash
awslocal s3api create-bucket --bucket hello-heart-covid-data
```
Confirm that the bucket was created by running:
```bash
awslocal s3api list-buckets | grep hello-heart-covid-data
```

For some reason I cant seem to use the `boto3` client with the localstack, so I will use the `awslocal` command line tool to interact with the S3 bucket in all the code. I think it requires a paid version but I am just running using the free tier.

## Create `providers` Dynamo Table 

To create the `providers` table in DynamoDB, I will use the following command:
```bash
awslocal dynamodb create-table \
    --table-name providers \
    --key-schema AttributeName=id,KeyType=HASH \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --region us-east-1
```
To list tables to confirm that the table was created, run:
```bash
awslocal dynamodb list-tables
```

## Notes

- I would have preferred to use RDS as the final desination for this data, however the free tier of 
LocalStack does not support RDS. I will instead opt to use Dynamo on LocalStack as the final destination for the data, 
even though the data is structured and would be better suited for a relational database or a warehouse.

- There are also clear rooms for improvements here in terms of parallelization of transforming and inserting the data
into the database. With the current setup, the data is processed and inserted into the database sequentially.

- Once nice thing about this though is that at each step of the pipeline the data is stored in S3, so if the pipeline fails
at any point, we can always restart the pipeline from the last successful step.