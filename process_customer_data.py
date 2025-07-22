
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

spark = SparkSession.builder.appName("CustomerDataPipeline").getOrCreate()


db_host = "your_db_server.database.windows.net" 
db_name = "your_database_name" 
db_user = "your_db_username"  
db_password = "your_db_password" 
db_table = "Customers" 

adls_customer_path = "abfss://raw-data@yourstorageaccount.dfs.core.windows.net/customer_data/"

print(f"Starting customer data processing at {datetime.now()}")

try:
    
    jdbc_url = f"jdbc:sqlserver://{db_host};database={db_name};"

    print(f"Reading data from table: {db_table}...")
    
    customer_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

   
    customer_count = customer_df.count()
    print(f"Customer record count: {customer_count}")

   
    if customer_count > 500:
        print(f"Customer count ({customer_count}) is greater than 500. Copying data to ADLS...")
        
        output_path = f"{adls_customer_path}snapshot_{datetime.now().strftime('%Y%m%d%H%M%S')}/"
       
        customer_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)
        print(f"Customer data successfully copied to: {output_path}")

        
        dbutils.notebook.exit(str(customer_count))
    else:
        print(f"Customer count ({customer_count}) is not greater than 500. Skipping copy to ADLS.")
        
        dbutils.notebook.exit("0")

except Exception as e:
  
    print(f"An error occurred during customer data processing: {e}")
  
    dbutils.notebook.exit("-1")

print(f"Finished customer data processing at {datetime.now()}")
