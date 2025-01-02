import unittest
from unittest.mock import patch, MagicMock
from src.data_fetching.save_weather_data_to_db import save_weather_data_to_db

from src.data_fetching.weather_fetcher import (
    WeatherFetcher,
    fetch_3_days_ago_data,
    fetch_last_5_years_data,
)

class TestSaveWeatherDataToDB(unittest.TestCase):

    @patch("src.data_fetching.save_weather_data_to_db.psycopg2.connect")
    @patch.dict("os.environ", {
        "DB_NAME": "test_db",
        "DB_USER": "test_user",
        "DB_PASSWORD": "test_password",
        "DB_HOST": "test_host",
        "DB_PORT": "5432"
    })
    def test_save_weather_data_to_db(self, mock_connect):
        # Mock database connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Example data
        example_data = {
            "hourly": {
                "time": ["2023-12-01T00:00", "2023-12-01T01:00"],
                "temperature_2m": [10.2, 9.8],
                "relative_humidity_2m": [85, 88],
                "dew_point_2m": [7.5, 7.2],
                "apparent_temperature": [9.0, 8.7],
                "precipitation": [0.0, 0.1],
                "rain": [0.0, 0.1],
            }
        }

        # Call the function
        save_weather_data_to_db(example_data, "Santa Rosa")

        # Assertions
        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="test_host",
            port="5432"
        )
        mock_cursor.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
