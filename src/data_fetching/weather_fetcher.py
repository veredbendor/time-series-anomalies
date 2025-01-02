import csv
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class WeatherFetcher:
    """
    Fetch weather data from Open-Meteo's Historical Weather API.
    """

    def __init__(self, base_url="https://archive-api.open-meteo.com/v1/era5"):
        self.base_url = base_url

    def fetch_historical_weather(self, lat, lon, start_date, end_date, hourly_params):
        """
        Fetch historical weather data for the given coordinates and date range.
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(hourly_params),
            "timezone": "auto",
        }
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            logging.info(f"Fetched data for {start_date} to {end_date} (lat={lat}, lon={lon})")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch weather data: {e}")
            return None


def save_weather_data_to_csv(data, filename, mode="w"):
    """
    Save weather data to a CSV file.
    """
    hourly_data = data.get("hourly", {})
    times = hourly_data.get("time", [])
    parameters = {key: hourly_data.get(key, []) for key in [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "precipitation", "rain"
    ]}
    header = ["time"] + list(parameters.keys())

    try:
        with open(filename, mode="a", newline="") as file:
            writer = csv.writer(file)
            if mode == "w":
                writer.writerow(header)  # Write header only if in write mode
            writer.writerows(zip(times, *parameters.values()))
        logging.info(f"Weather data saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving data to CSV: {e}")


def fetch_weather_data(lat, lon, start_date, end_date, filename=None):
    """
    Fetch and optionally save historical weather data for a specified date range.
    """
    fetcher = WeatherFetcher()
    hourly_params = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "precipitation", "rain"
    ]
    weather_data = fetcher.fetch_historical_weather(lat, lon, start_date, end_date, hourly_params)

    if weather_data:
        if filename:
            save_weather_data_to_csv(weather_data, filename)
        else:
            logging.info("Fetched weather data:")
            logging.info(weather_data)
    else:
        logging.error(f"Failed to fetch weather data for {start_date} to {end_date}.")


def fetch_last_5_years_data(lat, lon, filename=None):
    today = datetime.now()
    if not filename:
        filename = f"last_5_years_weather_data_{today.strftime('%Y-%m-%d')}.csv"

    start_date = today - timedelta(days=5 * 365)
    current_date = start_date

    while current_date < today:
        next_date = min(current_date + timedelta(days=30), today)
        start_str, end_str = current_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")
        fetch_weather_data(lat, lon, start_str, end_str, filename)
        current_date = next_date


def fetch_3_days_ago_data(lat, lon, filename=None):
    """
    Fetch weather data for 3 days ago and optionally save it to a CSV file.
    """
    three_days_ago = datetime.now() - timedelta(days=3)
    start_date = three_days_ago.strftime("%Y-%m-%d")
    end_date = start_date  # Single day

    hourly_params = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "precipitation", "rain",
    ]

    fetcher = WeatherFetcher()
    weather_data = fetcher.fetch_historical_weather(lat, lon, start_date, end_date, hourly_params)

    if weather_data:
        if filename:
            save_weather_data_to_csv(weather_data, filename)
        else:
            logging.info("Fetched weather data for 3 days ago:")
            logging.info(weather_data)
        return weather_data
    else:
        logging.error(f"Failed to fetch weather data for {start_date}.")
        return None
    
    
def save_weather_data_to_db(data, city):
    """
    Save weather data to the PostgreSQL database in the 'public' schema.
    """
    # Load database configuration from environment variables
    # db_config = {
    #     "dbname": os.getenv("DB_NAME"),
    #     "user": os.getenv("DB_USER"),
    #     "password": os.getenv("DB_PASSWORD"),
    #     "host": os.getenv("DB_HOST"),
    #     "port": os.getenv("DB_PORT"),
    # }
    
    db_config = {
        "dbname": "weather_data",
        "user": "weather_user",
        "password": "weather_password",
        "host": "localhost",
        "port": "5433",  # Adjust if needed
    }

    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    hourly_data = data.get("hourly", {})
    times = hourly_data.get("time", [])
    parameters = {
        key: hourly_data.get(key, [])
        for key in [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
        ]
    }

    try:
        for i, time in enumerate(times):
            cursor.execute(
                """
                INSERT INTO public.weather (
                    city, date, time, temperature, relative_humidity,
                    dew_point, apparent_temperature, precipitation, rain
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    city,
                    time.split("T")[0],  # Extract date
                    time,  # Full timestamp
                    parameters["temperature_2m"][i],
                    parameters["relative_humidity_2m"][i],
                    parameters["dew_point_2m"][i],
                    parameters["apparent_temperature"][i],
                    parameters["precipitation"][i],
                    parameters["rain"][i],
                )
            )
        connection.commit()
        print("Weather data saved to the database.")
    except Exception as e:
        print(f"Error saving data to the database: {e}")
    finally:
        cursor.close()
        connection.close()
