import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load environment variables from .env file
load_dotenv()

def save_weather_data_to_db(data, city):
    """
    Save weather data to the PostgreSQL database in the 'public' schema.
    """
    # Load database configuration from environment variables
    load_dotenv()
    db_config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
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
