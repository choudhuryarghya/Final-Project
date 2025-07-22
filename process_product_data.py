# Databricks notebook source
# Databricks Notebook: process_product_data.py
# This notebook fetches product data conditionally based on a 'customer_count' parameter
# passed from ADF.

# Import necessary libraries
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark Session (already available in Databricks)
spark = SparkSession.builder.appName("ProductDataPipeline").getOrCreate()

# --- Get Parameters from ADF ---
# This line creates a widget to receive the 'customer_count' parameter from ADF.
# The second argument is a default value if the parameter is not provided (useful for testing).
dbutils.widgets.text("customer_count", "0", "Customer Count from Parent")
# Retrieve the value of the 'customer_count' parameter
customer_count_param = dbutils.widgets.get("customer_count")
print(f"Received customer_count parameter from parent pipeline: {customer_count_param}")

# --- Configuration Parameters (REPLACE WITH YOUR ACTUAL VALUES) ---
# Database connection details (assuming same DB as customer data for simplicity)
db_host = "your_db_server.database.windows.net"
db_name = "your_database_name"
db_user = "your_db_username"
db_password = "your_db_password" # IMPORTANT: Use Databricks Secrets in production!
db_table = "Products" # The name of your product table

# ADLS Gen2 path for product data
# IMPORTANT: Replace <container_name> and <storage_account_name> with your actual ADLS Gen2 details.
adls_product_path = "abfss://raw-data@yourstorageaccount.dfs.core.windows.net/product_data/"
# If mounted: adls_product_path = "/mnt/raw_data/product_data/"

# --- Main Logic ---
print(f"Starting product data processing at {datetime.now()}")

try:
    # Convert the received parameter (which is a string) to an integer for comparison
    customer_count_int = int(customer_count_param)

    # Conditional copy based on customer_count: only if customer record count is > 600
    if customer_count_int > 600:
        print(f"Customer count ({customer_count_int}) is greater than 600. Copying product data to ADLS...")

        # Construct JDBC URL
        jdbc_url = f"jdbc:sqlserver://{db_host};database={db_name};"

        print(f"Reading data from table: {db_table}...")
        # Read product data from the database into a Spark DataFrame
        product_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()

        # Define output path with a timestamp for uniqueness
        output_path = f"{adls_product_path}snapshot_{datetime.now().strftime('%Y%m%d%H%M%S')}/"
        # Write the product DataFrame to ADLS Gen2 in Parquet format
        product_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
        print(f"Product data successfully copied to: {output_path}")
    else:
        print(f"Customer count ({customer_count_int}) is not greater than 600. Skipping product data copy.")

except ValueError:
    # Handle cases where the customer_count_param cannot be converted to an integer
    print(f"Error: Invalid customer_count parameter received: '{customer_count_param}'. Must be an integer.")
except Exception as e:
    # Catch any other unexpected errors
    print(f"An error occurred during product data processing: {e}")

print(f"Finished product data processing at {datetime.now()}")
