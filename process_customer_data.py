# Databricks notebook source
# Databricks Notebook: process_customer_data.py
# This notebook fetches customer data, counts records, and conditionally saves to ADLS Gen2.
# It also returns the customer count to ADF.

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Initialize Spark Session (already available in Databricks)
spark = SparkSession.builder.appName("CustomerDataPipeline").getOrCreate()

# --- Configuration Parameters (REPLACE WITH YOUR ACTUAL VALUES) ---
# Database connection details for your Azure SQL Database (or other relational DB)
db_host = "your_db_server.database.windows.net" # e.g., my-sql-server.database.windows.net
db_name = "your_database_name" # e.g., SalesDB
db_user = "your_db_username"   # e.g., sqladmin
db_password = "your_db_password" # IMPORTANT: Use Databricks Secrets in production!
db_table = "Customers" # The name of your customer table

# ADLS Gen2 path for customer data
# IMPORTANT: Replace <container_name> and <storage_account_name> with your actual ADLS Gen2 details.
adls_customer_path = "abfss://raw-data@yourstorageaccount.dfs.core.windows.net/customer_data/"
# If mounted: adls_customer_path = "/mnt/raw_data/customer_data/"

# --- Main Logic ---
print(f"Starting customer data processing at {datetime.now()}")

try:
    # Construct JDBC URL for connecting to the database
    jdbc_url = f"jdbc:sqlserver://{db_host};database={db_name};"

    print(f"Reading data from table: {db_table}...")
    # Read customer data from the database into a Spark DataFrame
    customer_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

    # Get the total number of records in the customer DataFrame
    customer_count = customer_df.count()
    print(f"Customer record count: {customer_count}")

    # Conditional copy to ADLS Gen2: only if record count is more than 500
    if customer_count > 500:
        print(f"Customer count ({customer_count}) is greater than 500. Copying data to ADLS...")
        # Define output path with a timestamp for uniqueness (e.g., customer_data/snapshot_20250721235959/)
        output_path = f"{adls_customer_path}snapshot_{datetime.now().strftime('%Y%m%d%H%M%S')}/"
        # Write the customer DataFrame to ADLS Gen2 in Parquet format
        customer_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
        print(f"Customer data successfully copied to: {output_path}")

        # Return the customer count to ADF.
        # dbutils.notebook.exit() sends a string value back to the calling ADF activity.
        dbutils.notebook.exit(str(customer_count))
    else:
        print(f"Customer count ({customer_count}) is not greater than 500. Skipping copy to ADLS.")
        # Return "0" to indicate that the condition was not met or no data was copied.
        dbutils.notebook.exit("0")

except Exception as e:
    # Catch any errors during the process and print them
    print(f"An error occurred during customer data processing: {e}")
    # Return "-1" to indicate an error state to ADF.
    dbutils.notebook.exit("-1")

print(f"Finished customer data processing at {datetime.now()}")