# COVID-19 Country-wise Data Analysis

This project fetches COVID-19 data for selected countries from the [disease.sh API](https://disease.sh/docs/#/COVID-19%3A%20Worldometers/get_v3_covid_19_countries) and performs data analysis using PySpark DataFrame. Additionally, it developes an API using Flask to display the analyzed data.

## Overview

The project consists of the following main components:

1. **Data Fetching**: Utilizes the disease.sh API to fetch COVID-19 data for 20 countries.

2. **Data Processing**: Processes the fetched data using PySpark DataFrame to perform required operations such as filtering, aggregation, and transformation.

3. **API Development**: Develops a Flask application to create a development server and expose APIs to display the analyzed data.

4. **Data Visualization**: Presents the analyzed data through API endpoints in JSON format for visualization or further consumption.

## How to Use

To run the project locally, follow these steps:

```bash
# Clone the repository:
git clone <repository-url>

# Navigate to the project directory:
cd <project-directory>

# Install the required packages using pip:
pip install -r requirements.txt

# Run the Flask application:
python src/covid_api.py

# Run the Main application:
python main.py

```

Now different api endpoints can be accessed on

`http://127.0.0.1:5000/{route}`