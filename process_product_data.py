


from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ProductDataPipeline").getOrCreate()

dbutils.widgets.text("customer_count", "0", "Customer Count from Parent")

customer_count_param = dbutils.widgets.get("customer_count")
print(f"Received customer_count parameter from parent pipeline: {customer_count_param}")


db_host = "your_db_server.database.windows.net"
db_name = "your_database_name"
db_user = "your_db_username"
db_password = "your_db_password" 
db_table = "Products" 

adls_product_path = "abfss://raw-data@yourstorageaccount.dfs.core.windows.net/product_data/"

print(f"Starting product data processing at {datetime.now()}")

try:

    customer_count_int = int(customer_count_param)

    
    if customer_count_int > 600:
        print(f"Customer count ({customer_count_int}) is greater than 600. Copying product data to ADLS...")

        
        jdbc_url = f"jdbc:sqlserver://{db_host};database={db_name};"

        print(f"Reading data from table: {db_table}...")
       
        product_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", db_table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()

        
        output_path = f"{adls_product_path}snapshot_{datetime.now().strftime('%Y%m%d%H%M%S')}/"

        product_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
        print(f"Product data successfully copied to: {output_path}")
    else:
        print(f"Customer count ({customer_count_int}) is not greater than 600. Skipping product data copy.")

except ValueError:
    
    print(f"Error: Invalid customer_count parameter received: '{customer_count_param}'. Must be an integer.")
except Exception as e:
    
    print(f"An error occurred during product data processing: {e}")

print(f"Finished product data processing at {datetime.now()}")
