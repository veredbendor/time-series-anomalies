from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_fetching.weather_fetcher import fetch_last_5_years_data

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
    dag_id="fetch_last_5_years_weather_data",
    default_args=default_args,
    description="Fetch last 5 years of weather data and save to a CSV file",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def fetch_weather():
        """
        Task to fetch last 5 years of weather data and save it to a CSV file.
        """
        today = datetime.now()
        filename = f"last_5_years_weather_data_{today.strftime('%Y-%m-%d')}.csv"
        fetch_last_5_years_data(38.4405, -122.7144, filename)

    # Define the PythonOperator
    fetch_weather_task = PythonOperator(
        task_id="fetch_last_5_years_data",
        python_callable=fetch_weather,
    )
