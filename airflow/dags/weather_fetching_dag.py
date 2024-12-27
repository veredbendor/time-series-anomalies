from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from data_fetching.weather_fetcher import WeatherFetcher

def fetch_weather_data():
    """
    Fetch weather data for a specified city.
    The API key is managed within the WeatherFetcher class.
    """
    fetcher = WeatherFetcher()  # Automatically fetches API key from the environment
    city = "London"  # Specify the city
    weather_data = fetcher.fetch_weather(city)
    if weather_data:
        print(f"Weather data for {city}: {weather_data}")
    else:
        print(f"Failed to fetch weather data for {city}")

# Default arguments for the DAG
default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="weather_fetching_dag",
    default_args=default_args,
    schedule_interval=None,  # On-demand only
    catchup=False,
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )
