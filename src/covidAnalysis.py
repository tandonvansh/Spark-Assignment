from pyspark.sql import DataFrame


def show_data(df: DataFrame, num_rows: int = 5):
    """
    Display the first few rows of a Spark DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame.
        num_rows (int): Number of rows to display. Default is 5.
    """
    df.show(num_rows)


def most_affected_country(df: DataFrame) -> str:
    """
    Find the most affected country (total death/total covid cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the most affected country.
    """
    most_affected_country = df.withColumn("affected_ratio", df["deaths"] / df["cases"]) \
        .orderBy("affected_ratio", ascending=False).first()["country"]
    return most_affected_country


def least_affected_country(df: DataFrame) -> str:
    """
    Find the least affected country (total death/total covid cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the least affected country.
    """
    least_affected_country = df.withColumn("affected_ratio", df["deaths"] / df["cases"]) \
        .orderBy("affected_ratio").first()["country"]
    return least_affected_country


def country_with_highest_cases(df: DataFrame) -> str:
    """
    Find the country with the highest number of COVID-19 cases.

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country with the highest cases.
    """
    country_with_highest_cases = df.orderBy("cases", ascending=False).first()["country"]
    return country_with_highest_cases


def country_with_minimum_cases(df: DataFrame) -> str:
    """
    Find the country with the minimum number of COVID-19 cases.

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country with the minimum cases.
    """
    country_with_minimum_cases = df.orderBy("cases").first()["country"]
    return country_with_minimum_cases


def total_cases(df: DataFrame) -> int:
    """
    Calculate the total number of COVID-19 cases.

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        int: The total number of cases.
    """
    total_cases = df.agg({"cases": "sum"}).collect()[0][0]
    return total_cases


def most_efficient_country(df: DataFrame) -> str:
    """
    Find the country that handled COVID-19 most efficiently (total recovery/total covid cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country that handled COVID-19 most efficiently.
    """
    most_efficient_country = df.withColumn("recovery_ratio", df["recovered"] / df["cases"]) \
        .orderBy("recovery_ratio", ascending=False).first()["country"]
    return most_efficient_country


def least_efficient_country(df: DataFrame) -> str:
    """
    Find the country that handled COVID-19 least efficiently (total recovery/total covid cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country that handled COVID-19 least efficiently.
    """
    least_efficient_country = df.withColumn("recovery_ratio", df["recovered"] / df["cases"]) \
        .orderBy("recovery_ratio").first()["country"]
    return least_efficient_country


def least_suffering_country(df: DataFrame) -> str:
    """
    Find the country least suffering from COVID-19 (least critical cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country least suffering from COVID-19.
    """
    least_suffering_country = df.orderBy("critical").first()["country"]
    return least_suffering_country


def still_suffering_country(df: DataFrame) -> str:
    """
    Find the country still suffering from COVID-19 (highest critical cases).

    Args:
        df (DataFrame): The Spark DataFrame containing the data.

    Returns:
        str: The name of the country still suffering from COVID-19.
    """
    still_suffering_country = df.orderBy("critical", ascending=False).first()["country"]
    return still_suffering_country
