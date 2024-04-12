# Hello Heart Take Home

Airflow ETL Assignment for Hello Heart. This project is a simple ETL pipeline that reads data from a CSV file, processes it using Airflow, and writes it to a MySQL database.

## Start Up Airflow Server

Note: This will only work with earlier versions of python, not the latest (3.10 and on).

The current path I have to Python 3.8 is installed at:
```
/opt/homebrew/opt/python@3.8/libexec/bin
```

Set AIRFLOW_HOME:
```
export AIRFLOW_HOME="/Users/nicholas/Code/airflow/Airflow/.venv/lib/python3.8/site-packages/airflow"
```
Initialize venv and install requirements:
```
$ python3.8 -m venv .venv
$ source .venv/bin/activate
(.venv) $ pip install --upgrade pip
(.venv) $ pip install -r requirements.txt
```
Install Airflow Separately:
```
(.venv) $ AIRFLOW_VERSION=2.8.1 && \
    PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)" && \
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" && \
    pip install "apache-airflow[async,postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```
Initialize Airflow Database w/ Admin User:
```
(.venv) $ airflow db init
(.venv) $ airflow users create \
    --username admin \
    --firstname nick \
    --lastname roberson \
    --role Admin \
    --email nicholas.roberson.95@gmail.com
```

Start Airflow Server (Terminal Window 1):
```
(.venv) $ airflow webserver
```

Start Airflow Scheduler (Terminal Window 2):
```
(.venv) $ airflow scheduler
```

## Docs Source

- https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html