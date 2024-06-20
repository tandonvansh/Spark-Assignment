import os
import logging
import json
from flask import Flask, jsonify
from covidAnalysis import *
from sparkUtils import read_csv, create_spark_session

# Initialize Flask
app = Flask(__name__)

# Load configurations from config.json
with open('../config.json', 'r') as f:
    config = json.load(f)

# Set up logging
log_directory = config['logs']['directory']
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file = os.path.join(log_directory, config['logs']['file'])
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = create_spark_session()

# Read the CSV file into a DataFrame
df = read_csv(spark, config['spark']['data_file_path'])


def log_activity(api_name, result):
    logging.info(f"API '{api_name}' was accessed. Result: {result}")


@app.route('/covid-data', methods=['GET'])
def covid_data():
    try:
        json_data = df.toJSON()  # Convert DataFrame to JSON string
        parsed_data = [json.loads(line) for line in json_data.collect()]  # Parse JSON strings
        return jsonify(parsed_data)
    except Exception as e:
        logging.error(f"Error processing request for /covid-data: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/most-affected-country', methods=['GET'])
def most_affected_country_api():
    try:
        result = most_affected_country(df)
        log_activity("most-affected-country", result)
        return jsonify({"most_affected_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /most-affected-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/least-affected-country', methods=['GET'])
def least_affected_country_api():
    try:
        result = least_affected_country(df)
        log_activity("least-affected-country", result)
        return jsonify({"least_affected_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /least-affected-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/country-with-highest-cases', methods=['GET'])
def country_with_highest_cases_api():
    try:
        result = country_with_highest_cases(df)
        log_activity("country-with-highest-cases", result)
        return jsonify({"country_with_highest_cases": result})
    except Exception as e:
        logging.error(f"Error processing request for /country-with-highest-cases: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/country-with-minimum-cases', methods=['GET'])
def country_with_minimum_cases_api():
    try:
        result = country_with_minimum_cases(df)
        log_activity("country-with-minimum-cases", result)
        return jsonify({"country_with_minimum_cases": result})
    except Exception as e:
        logging.error(f"Error processing request for /country-with-minimum-cases: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/total-cases', methods=['GET'])
def total_cases_api():
    try:
        result = total_cases(df)
        log_activity("total-cases", result)
        return jsonify({"total_cases": result})
    except Exception as e:
        logging.error(f"Error processing request for /total-cases: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/most-efficient-country', methods=['GET'])
def most_efficient_country_api():
    try:
        result = most_efficient_country(df)
        log_activity("most-efficient-country", result)
        return jsonify({"most_efficient_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /most-efficient-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/least-efficient-country', methods=['GET'])
def least_efficient_country_api():
    try:
        result = least_efficient_country(df)
        log_activity("least-efficient-country", result)
        return jsonify({"least_efficient_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /least-efficient-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/least-suffering-country', methods=['GET'])
def least_suffering_country_api():
    try:
        result = least_suffering_country(df)
        log_activity("least-suffering-country", result)
        return jsonify({"least_suffering_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /least-suffering-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


@app.route('/still-suffering-country', methods=['GET'])
def still_suffering_country_api():
    try:
        result = still_suffering_country(df)
        log_activity("still-suffering-country", result)
        return jsonify({"still_suffering_country": result})
    except Exception as e:
        logging.error(f"Error processing request for /still-suffering-country: {e}")
        return jsonify({"error": "An error occurred while processing the request"}), 500


if __name__ == '__main__':
    # Run the Flask app
    app.run(debug=True)
