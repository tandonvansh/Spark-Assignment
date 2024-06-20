import requests
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


def fetch_covid_data_and_save_to_csv(url, output_file):
    """
    Fetch COVID-19 data from the given API endpoint, extract required columns,
    and save the data to a CSV file.

    Args:
        url (str): The URL of the API endpoint.
        output_file (str): The path to save the CSV file.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        data = response.json()

        # Extract required columns
        columns = ["country", "cases", "todayCases", "deaths", "todayDeaths", "recovered", "todayRecovered",
                   "active", "critical", "casesPerOneMillion", "deathsPerOneMillion", "tests",
                   "testsPerOneMillion", "population", "continent", "oneCasePerPeople", "oneDeathPerPeople",
                   "oneTestPerPeople", "activePerOneMillion", "recoveredPerOneMillion", "criticalPerOneMillion"]

        # Create a DataFrame
        df = pd.DataFrame(data)
        df = df[columns]

        # Write DataFrame to CSV file
        df.to_csv(output_file, index=False)
        logging.info(f"COVID-19 data saved to {output_file}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching COVID-19 data from {url}: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
