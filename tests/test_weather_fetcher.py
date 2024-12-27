import unittest
from unittest.mock import patch, mock_open

from datetime import datetime
import requests

from src.data_fetching.weather_fetcher import (
    WeatherFetcher,
    fetch_3_days_ago_data,
    fetch_last_5_years_data,
)



class TestWeatherFetcher(unittest.TestCase):
    @patch("src.data_fetching.weather_fetcher.requests.get")
    def test_fetch_weather_success(self, mock_get):
        # Mock successful API response
        mock_response = {
            "hourly": {
                "temperature_2m": [22.5],
                "time": ["2024-12-24T00:00"],
            }
        }
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        fetcher = WeatherFetcher()
        result = fetcher.fetch_historical_weather(
            lat=38.4405,
            lon=-122.7144,
            start_date="2024-12-24",
            end_date="2024-12-24",
            hourly_params=["temperature_2m"]
        )
        self.assertIsNotNone(result)
        self.assertIn("hourly", result)
        self.assertEqual(result["hourly"]["temperature_2m"][0], 22.5)

    @patch("src.data_fetching.weather_fetcher.requests.get")
    def test_fetch_weather_failure(self, mock_get):
        # Mock failed API response
        mock_response = mock_get.return_value
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("API Error")
        mock_response.status_code = 400  # Simulate a 400 Bad Request status

        fetcher = WeatherFetcher()
        result = fetcher.fetch_historical_weather(
            lat=38.4405,
            lon=-122.7144,
            start_date="Invalid",
            end_date="Invalid",
            hourly_params=["temperature_2m"]
        )
        self.assertIsNone(result)
        
import unittest
from unittest.mock import patch
from src.data_fetching.weather_fetcher import fetch_3_days_ago_data

class TestFetch3DaysAgoData(unittest.TestCase):
    @patch("src.data_fetching.weather_fetcher.save_weather_data_to_csv")
    @patch("src.data_fetching.weather_fetcher.WeatherFetcher.fetch_historical_weather")
    def test_fetch_3_days_ago_data_success_no_file(self, mock_fetch_historical_weather, mock_save_to_csv):
        # Mock successful weather data
        mock_weather_data = {
            "hourly": {
                "time": ["2024-12-24T00:00", "2024-12-24T01:00"],
                "temperature_2m": [14.2, 13.9],
            }
        }
        mock_fetch_historical_weather.return_value = mock_weather_data

        # Call the function without a filename
        result = fetch_3_days_ago_data(38.4405, -122.7144)

        # Verify behavior
        self.assertIsNotNone(result)
        self.assertEqual(result, mock_weather_data)
        mock_fetch_historical_weather.assert_called_once()
        mock_save_to_csv.assert_not_called()

    @patch("src.data_fetching.weather_fetcher.save_weather_data_to_csv")
    @patch("src.data_fetching.weather_fetcher.WeatherFetcher.fetch_historical_weather")
    def test_fetch_3_days_ago_data_success_with_file(self, mock_fetch_historical_weather, mock_save_to_csv):
        # Mock successful weather data
        mock_weather_data = {
            "hourly": {
                "time": ["2024-12-24T00:00", "2024-12-24T01:00"],
                "temperature_2m": [14.2, 13.9],
            }
        }
        mock_fetch_historical_weather.return_value = mock_weather_data

        # Call the function with a filename
        result = fetch_3_days_ago_data(38.4405, -122.7144, filename="test.csv")

        # Verify behavior
        self.assertIsNotNone(result)
        self.assertEqual(result, mock_weather_data)
        mock_fetch_historical_weather.assert_called_once()
        mock_save_to_csv.assert_called_once_with(mock_weather_data, "test.csv")

    @patch("src.data_fetching.weather_fetcher.WeatherFetcher.fetch_historical_weather")
    def test_fetch_3_days_ago_data_failure(self, mock_fetch_historical_weather):
        # Mock failed weather data fetch
        mock_fetch_historical_weather.return_value = None

        # Call the function
        result = fetch_3_days_ago_data(38.4405, -122.7144)

        # Verify behavior
        self.assertIsNone(result)
        mock_fetch_historical_weather.assert_called_once()

class TestFetchLast5YearsData(unittest.TestCase):
    
    @patch("src.data_fetching.weather_fetcher.save_weather_data_to_csv")
    @patch("src.data_fetching.weather_fetcher.WeatherFetcher.fetch_historical_weather")
    def test_fetch_last_5_years_data(self, mock_fetch_historical_weather, mock_save_to_csv):
        # Mock successful weather data
        mock_weather_data = {
            "hourly": {
                "time": ["2024-12-01T00:00", "2024-12-01T01:00"],
                "temperature_2m": [14.2, 13.9],
            }
        }
        mock_fetch_historical_weather.return_value = mock_weather_data

        # Expected filename
        today = datetime.now()
        expected_filename = f"last_5_years_weather_data_{today.strftime('%Y-%m-%d')}.csv"

        # Call the function
        fetch_last_5_years_data(38.4405, -122.7144)

        # Verify save_weather_data_to_csv was called with the correct filename
        mock_save_to_csv.assert_called()
        args, _ = mock_save_to_csv.call_args  # Positional arguments
        assert args[1] == expected_filename  # Verify the filename
