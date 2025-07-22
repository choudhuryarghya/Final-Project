
import requests
import json
from datetime import datetime


countries = ["india", "us", "uk", "china", "russia"]

base_url = "https://restcountries.com/v3.1/name/"


adls_base_path = "abfss://raw-data@<yourstorageaccountname>.dfs.core.windows.net/countries/"


def fetch_and_save_country_data(country_name, path):

    try:
        url = f"{base_url}{country_name}"
        print(f"Fetching data for: {country_name} from {url}")

        
        response = requests.get(url)
        
        response.raise_for_status()

      
        data = response.json()

        
        file_name = f"{country_name.lower()}.json"
        
        full_path = f"{path}{file_name}"

       
        dbutils.fs.put(full_path, json.dumps(data, indent=4), overwrite=True)
        print(f"Successfully saved data for {country_name} to {full_path}")

    except requests.exceptions.RequestException as e:
        
        print(f"Error fetching data for {country_name}: {e}")
    except json.JSONDecodeError as e:
        
        print(f"Error decoding JSON for {country_name}: {e}")
    except Exception as e:
        
        print(f"An unexpected error occurred for {country_name}: {e}")


if __name__ == "__main__":
    print(f"Starting country data ingestion at {datetime.now()}")
   
    for country in countries:
        fetch_and_save_country_data(country, adls_base_path)
    print(f"Finished country data ingestion at {datetime.now()}")
