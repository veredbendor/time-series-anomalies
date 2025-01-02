from datetime import datetime
from data_fetching.weather_fetcher import fetch_last_5_years_data

def main():
    """
    Fetch the last 5 years of weather data and save it to a CSV file.
    """
    # Define the city coordinates
    latitude = 38.4405
    longitude = -122.7144

    # Generate a filename for the CSV file
    today = datetime.now()
    filename = f"/opt/airflow/last_5_years_weather_data_{today.strftime('%Y-%m-%d')}.csv"

    print(f"Fetching weather data for the last 5 years...")
    print(f"Saving the data to {filename}")

    # Call the function to fetch data and save it to the file
    fetch_last_5_years_data(latitude, longitude, filename)

    print("Weather data has been saved successfully.")

if __name__ == "__main__":
    main()
