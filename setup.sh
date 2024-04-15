# Download the docker-compose.yaml file only if it doesn't exist
if [ ! -f docker-compose.yaml ]; then
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.2/docker-compose.yaml'
fi

# Make expected directories and set an expected environment variable
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize the database
docker-compose up airflow-init
# Start up all services
docker-compose up