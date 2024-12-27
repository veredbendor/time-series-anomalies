from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_fetching.weather_fetcher import fetch_3_days_ago_data
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    dag_id="fetch_weather_data_3_days_ago",  # DAG ID
    default_args=default_args,
    description="Fetch historical weather data for 3 days ago and log the data",
    schedule_interval="0 0 * * *",  # Runs daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def fetch_weather_data():
        """
        Task to fetch weather data for 3 days ago and log the data.
        """
        latitude, longitude = 38.4405, -122.7144  # Coordinates for Santa Rosa, CA
        weather_data = fetch_3_days_ago_data(latitude, longitude)

        if weather_data:
            logging.info("Weather data for 3 days ago:")
            logging.info(weather_data)
        else:
            logging.warning("No weather data found for 3 days ago.")



    # Define the PythonOperator
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_data_3_days_ago",  # Ensure this matches
        python_callable=fetch_weather_data,
    )
