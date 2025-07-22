🚀 Generic Continuous Data Ingestion into Databricks
✨ Project Overview
This project demonstrates a robust and scalable data engineering pipeline built on Azure, designed for continuous data ingestion from various sources into Azure Databricks for processing and storage. It showcases the orchestration capabilities of Azure Data Factory and the powerful data processing features of Databricks, along with best practices for conditional execution and parameter passing.

The pipeline comprises two main flows:

Country Data Ingestion: Fetches country information from a REST API and lands it as JSON files in Azure Data Lake Storage Gen2 (ADLS Gen2).

Customer & Product Data Processing: Conditionally processes customer data from an Azure SQL Database, and if a certain threshold is met, triggers a child pipeline to process product data, also from Azure SQL Database.

🌟 Key Features
Automated Data Ingestion: Scheduled ingestion of country data from external REST APIs.

Conditional Data Processing: Dynamic processing of customer data based on record count, triggering further actions.

Parameterized Pipeline Execution: Passing dynamic values between parent and child pipelines in Azure Data Factory.

Scalable Data Storage: Utilizing Azure Data Lake Storage Gen2 for efficient raw and processed data storage.

Powerful Data Transformation: Leveraging Azure Databricks (PySpark) for complex data fetching and manipulation.

Orchestration & Scheduling: Azure Data Factory manages the end-to-end workflow and scheduling.

Version Control: Integration with GitHub for all pipeline definitions and notebook code.

🏗️ Architecture
The solution leverages a combination of Azure services to create a resilient data pipeline:

+-------------------+     +-------------------+     +---------------------+
|   REST API        |     |   Azure SQL DB    |     |   Azure Data Lake   |
| (restcountries.com)|     | (Customers, Products)|     |   Storage Gen2      |
+-------------------+     +-------------------+     +---------------------+
         |                         |                         |
         |                         |                         |
         v                         v                         v
+-----------------------------------------------------------------+
|                    Azure Data Factory (ADF)                     |
| +---------------------+   +---------------------+   +-----------------+ |
| | PL_Ingest_Country_Data|-->| PL_Process_Customer_Data|-->| PL_Copy_Product_Data| |
| | (Orchestrates API call) | | (Conditional Logic) | | (Parameterized) | |
| +---------------------+   +---------------------+   +-----------------+ |
+-----------------------------------------------------------------+
         |                                 |
         |                                 |
         v                                 v
+-----------------------------------------------------------------+
|                    Azure Databricks (PySpark)                   |
| +---------------------+   +---------------------+   +-----------------+ |
| | ingest_country_data |   | process_customer_data | | process_product_data| |
| | (Fetches & Saves JSON)| | (Counts & Saves Parquet)| | (Fetches & Saves Parquet)| |
| +---------------------+   +---------------------+   +-----------------+ |
+-----------------------------------------------------------------+
         |                                 |
         |                                 |
         v                                 v
+-----------------------------------------------------------------+
|                     GitHub (Version Control)                    |
| (ADF Pipelines, Linked Services, Databricks Notebooks)          |
+-----------------------------------------------------------------+

🛠️ Technologies Used
Azure Data Factory (ADF): 📊 Orchestration, scheduling, and pipeline management.

Azure Databricks: 💻 Spark-based analytics platform for running Python notebooks (PySpark).

Azure Data Lake Storage Gen2 (ADLS Gen2): ☁️ Scalable and secure data lake for storing raw JSON and processed Parquet files.

Azure SQL Database: 🗄️ Relational database for source customer and product data.

Python: 🐍 Primary language used for data processing logic within Databricks notebooks.

Apache Spark: 🔥 Distributed processing engine for large-scale data operations.

REST API: 🌐 External data source (https://restcountries.com/) for country data.

GitHub: 🐙 Version control system for all project code and configurations.

🚀 Project Setup: A Step-by-Step Journey
This section outlines the journey taken to build this project.

Phase 1: Azure Resource Provisioning & Basic Setup
Azure Subscription: Ensured an active Azure subscription.

Azure Databricks Workspace: Deployed a Databricks workspace.

Azure Data Lake Storage Gen2: Created an ADLS Gen2 account and a raw-data container with countries, customer_data, and product_data subfolders.

Azure SQL Database: Set up an Azure SQL Database with Customers (containing >500 records) and Products tables. Configured firewall rules to allow Databricks access.

Databricks Cluster: Ensured a running Databricks cluster for notebook execution.

ADF Linked Service to Databricks: Created AzureDatabricksLinkedService in ADF, connecting to the Databricks workspace using a Personal Access Token (PAT).

Phase 2: Databricks Notebook Development
Three Python notebooks were developed in Databricks:

ingest_country_data.py:

Purpose: Fetches data for India, US, UK, China, and Russia from https://restcountries.com/v3.1/name/{name}.

Output: Saves each country's data as a separate JSON file in abfss://raw-data@yourstorageaccount.dfs.core.windows.net/countries/.

Key Libraries: requests, json.

process_customer_data.py:

Purpose: Reads Customers data from Azure SQL Database.

Logic: Counts customer records. If customer_count > 500, it saves the data as Parquet in abfss://raw-data@yourstorageaccount.dfs.core.windows.net/customer_data/.

Return Value: Uses dbutils.notebook.exit(str(customer_count)) to return the count to ADF. Returns "0" if count is not > 500, or "-1" on error.

Key Libraries: pyspark.sql.

process_product_data.py:

Purpose: Reads Products data from Azure SQL Database.

Input: Expects a customer_count parameter from ADF via dbutils.widgets.text("customer_count", ...).

Logic: If the received customer_count > 600, it saves the product data as Parquet in abfss://raw-data@yourstorageaccount.dfs.core.windows.net/product_data/.

Key Libraries: pyspark.sql.

Phase 3: Azure Data Factory Pipeline Orchestration
Three pipelines were designed and configured in ADF:

PL_Ingest_Country_Data (Parent Pipeline):

Activity: RunCountryIngestionNotebook (Databricks Notebook activity).

Configuration: Linked to AzureDatabricksLinkedService, points to ingest_country_data notebook. No base parameters.

Trigger: Scheduled to run twice a day (00:00 IST and 12:00 IST).

PL_Copy_Product_Data (Child Pipeline):

Parameters: Defined a pipeline parameter named customerCountParam (Type: String).

Activity: RunProductProcessingNotebook (Databricks Notebook activity).

Configuration: Linked to AzureDatabricksLinkedService, points to process_product_data notebook.

Base Parameters: Passed customerCountParam to the notebook's customer_count widget using @pipeline().parameters.customerCountParam.

PL_Process_Customer_Data (Parent Pipeline):

Activity 1: RunCustomerProcessingNotebook (Databricks Notebook activity).

Configuration: Linked to AzureDatabricksLinkedService, points to process_customer_data notebook.

Activity 2: CheckCustomerCount (If Condition activity).

Dependency: Success arrow from RunCustomerProcessingNotebook.

Expression: @greater(int(activity('RunCustomerProcessingNotebook').output.runOutput), 600). This checks if the customer count returned by the notebook is greater than 600.

Activity 3 (inside CheckCustomerCount -> True branch): ExecuteProductPipeline (Execute Pipeline activity).

Invoked Pipeline: Selected PL_Copy_Product_Data.

Wait on completion: Checked.

Parameters: Passed customerCountParam with value @activity('RunCustomerProcessingNotebook').output.runOutput. This dynamically sends the customer count to the child pipeline.

Phase 4: GitHub Integration for Version Control
Both ADF and Databricks were integrated with GitHub to ensure all code and configurations are version-controlled.

ADF GitHub Integration:

Configured Git integration in ADF Studio (Author -> Set up code repository).

Authorized the "AzureDataFactory" OAuth app to connect to the GitHub repository.

Selected the repository and main branch as the collaboration branch.

Imported existing ADF resources to the repository.

Databricks Repos Integration:

Generated a GitHub Personal Access Token (PAT) with repo scope.

Configured Git credentials in Databricks (User Settings -> Linked accounts).

Created a Databricks Repo (e.g., my-data-engineering-repo) by cloning the GitHub repository.

Moved all ingest_country_data, process_customer_data, and process_product_data notebooks into the newly created repo folder within Databricks.

Committed and pushed these notebooks directly from the Databricks Repos UI to GitHub.

✅ How to Run and Verify
Trigger Pipelines:

In ADF Studio, go to the Monitor section.

For PL_Ingest_Country_Data and PL_Process_Customer_Data, click "Add trigger" -> "Trigger Now".

Monitor ADF Runs:

Stay in the Monitor section to observe pipeline and activity statuses (Success/Failure).

Click on failed runs to view detailed error messages in the "Output" section of the activity.

Check Databricks Notebook Logs:

In Databricks UI, go to Workflows -> Job runs.

Click on the specific job run for your notebooks (e.g., ingest_country_data).

Review the cell outputs for print statements and any error messages from your Python code.

Verify Data in ADLS Gen2:

Check your raw-data container in ADLS Gen2 for:

countries/ folder with JSON files (e.g., india.json).

customer_data/ folder with Parquet files (if customer count > 500).

product_data/ folder with Parquet files (if customer count > 600).

💡 Future Enhancements
Robust Error Handling & Alerting: Implement email or Teams notifications for pipeline failures using Azure Logic Apps or Azure Functions.

Centralized Logging: Integrate ADF and Databricks logs with Azure Log Analytics for comprehensive monitoring and Kusto queries.

Data Quality Framework: Incorporate a dedicated data quality framework (e.g., Great Expectations) within Databricks notebooks.

Parameterization of Sensitive Info: Use Azure Key Vault for all database credentials and other sensitive information.

CI/CD Pipeline: Set up automated CI/CD pipelines using Azure DevOps or GitHub Actions to deploy ADF and Databricks changes.

Delta Lake: Convert data storage in ADLS Gen2 to Delta Lake format for ACID transactions, schema enforcement, and time travel capabilities.

Data Visualization Dashboard: Connect processed data to a BI tool like Power BI or Tableau for interactive dashboards.

This README.md should give a fantastic overview of your project! Let me know if you'd like any adjustments or further details added.
