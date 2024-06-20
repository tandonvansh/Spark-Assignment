import json
from src.covidAnalysis import (
    show_data,
    most_affected_country,
    least_affected_country,
    country_with_highest_cases,
    country_with_minimum_cases,
    total_cases,
    most_efficient_country,
    least_efficient_country,
    least_suffering_country,
    still_suffering_country
)
from src.getCovidData import fetch_covid_data_and_save_to_csv
from src.sparkUtils import create_spark_session, read_csv, verifyAPI


def main():
    """
    Main function to execute the COVID-19 data analysis and API verification.

    This function loads configurations from a JSON file, creates a SparkSession, fetches COVID-19 data from an API,
    saves the data to a CSV file, reads the CSV file into a DataFrame, performs various data analysis tasks,
    and verifies the APIs created in covid_api.py.

    :return: None
    """
    try:
        # Load configurations from config.json
        with open('config.json', 'r') as f:
            config = json.load(f)

        # Create a SparkSession
        spark = create_spark_session()

        # Define API endpoint URL and output CSV file path
        url = config['api']['url']
        output_file = config['spark']['data_file_path'][3:]

        # Fetch COVID data and save to CSV
        fetch_covid_data_and_save_to_csv(url, output_file)

        # Read the CSV file into a DataFrame
        df = read_csv(spark, output_file)

        # Perform analysis
        show_data(df)
        print("2.1) Most affected country (total death/total covid cases):", most_affected_country(df))
        print("2.2) Least affected country (total death/total covid cases):", least_affected_country(df))
        print("2.3) Country with highest covid cases:", country_with_highest_cases(df))
        print("2.4) Country with minimum covid cases:", country_with_minimum_cases(df))
        print("2.5) Total cases:", total_cases(df))
        print("2.6) Country that handled the covid most efficiently (total recovery/total covid cases):",
              most_efficient_country(df))
        print("2.7) Country that handled the covid least efficiently (total recovery/total covid cases):",
              least_efficient_country(df))
        print("2.8) Country least suffering from covid (least critical cases):", least_suffering_country(df))
        print("2.9) Country still suffering from covid (highest critical cases):", still_suffering_country(df))

        # Verifying APIs Created in covid_api.py
        # Call the verifyAPI function for each endpoint and print the data
        endpoints = [
            "most-affected-country",
            "least-affected-country",
            "country-with-highest-cases",
            "country-with-minimum-cases",
            "total-cases",
            "most-efficient-country",
            "least-efficient-country",
            "least-suffering-country",
            "still-suffering-country"
        ]

        for endpoint in endpoints:
            print(f"Verifying endpoint: {endpoint}")
            data = verifyAPI(endpoint)
            if data:
                print(f"{data}")
            else:
                print(f"Failed to verify endpoint: {endpoint}")
            print("-" * 50)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Stop Spark session
        spark.stop()


if __name__ == "__main__":
    main()
