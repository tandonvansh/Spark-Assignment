from pyspark.sql import SparkSession
import requests


def create_spark_session():
    """
    Create and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName("COVID Data Analysis") \
        .getOrCreate()
    return spark


def read_csv(spark, file_path):
    """
    Read a CSV file into a DataFrame using the provided SparkSession.

    Args:
        spark (SparkSession): The SparkSession object.
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: The DataFrame containing the data from the CSV file.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df


def verifyAPI(route):
    """
    This function sends a GET request to a specified route of a local API server.
    It handles exceptions and returns the JSON response if the request is successful.

    Parameters:
    route (str): The route to be appended to the base URL of the API server.

    Returns:
    dict or None: The JSON response from the API server if the request is successful.
                  Returns None if an error occurs during the request.

    Raises:
    requests.exceptions.RequestException: If an error occurs while sending the request.
    """
    url = f"http://127.0.0.1:5000/{route}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for non-200 status codes
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data from {url}: {e}")
        return None