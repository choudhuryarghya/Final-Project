# Databricks notebook source
# Databricks Notebook: ingest_country_data.py
# This notebook fetches country data from a REST API and saves it as JSON files in ADLS Gen2.

# Import necessary libraries
import requests
import json
from datetime import datetime

# --- Configuration Parameters ---
# Define the list of countries to fetch data for
countries = ["india", "us", "uk", "china", "russia"]
# Base URL for the REST Countries API
base_url = "https://restcountries.com/v3.1/name/"

# Define the ADLS Gen2 path where JSON files will be stored.
# IMPORTANT: Replace <container_name> and <storage_account_name> with your actual ADLS Gen2 details.
# Example using ABFS path:
adls_base_path = "abfss://raw-data@<yourstorageaccountname>.dfs.core.windows.net/countries/"

# If you have mounted ADLS Gen2, your path might look like:
# adls_base_path = "/mnt/raw-data/countries/"
# Ensure your mount point is correctly configured if you use this.

# --- Function to fetch data from API and save to ADLS ---
def fetch_and_save_country_data(country_name, path):
    """
    Fetches country data from the REST API and saves it as a JSON file in ADLS Gen2.
    """
    try:
        url = f"{base_url}{country_name}"
        print(f"Fetching data for: {country_name} from {url}")

        # Make the HTTP GET request to the API
        response = requests.get(url)
        # Raise an exception for HTTP errors (4xx or 5xx status codes)
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        # Construct the file name (e.g., "india.json")
        file_name = f"{country_name.lower()}.json"
        # Construct the full path in ADLS Gen2
        full_path = f"{path}{file_name}"

        # Save data as JSON to ADLS Gen2 using dbutils.fs.put
        # dbutils.fs.put writes the string content directly to the specified path.
        # `overwrite=True` ensures that if the file already exists, it will be replaced.
        dbutils.fs.put(full_path, json.dumps(data, indent=4), overwrite=True)
        print(f"Successfully saved data for {country_name} to {full_path}")

    except requests.exceptions.RequestException as e:
        # Handle errors related to the HTTP request (e.g., network issues, invalid URL)
        print(f"Error fetching data for {country_name}: {e}")
    except json.JSONDecodeError as e:
        # Handle errors if the API response is not valid JSON
        print(f"Error decoding JSON for {country_name}: {e}")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred for {country_name}: {e}")

# --- Main execution block ---
if __name__ == "__main__":
    print(f"Starting country data ingestion at {datetime.now()}")
    # Loop through each country and call the function to fetch and save its data
    for country in countries:
        fetch_and_save_country_data(country, adls_base_path)
    print(f"Finished country data ingestion at {datetime.now()}")
