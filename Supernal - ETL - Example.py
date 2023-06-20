# Databricks notebook source
# DBTITLE 1,List Databricks Live Streaming Events
files = dbutils.fs.ls("/databricks-datasets/structured-streaming/events")
print(f"structured streaming total files: {len(files)}")

# COMMAND ----------

# DBTITLE 1,Get Personal Access Token, User Name, and Databricks Instance
# authentication parameters
databricks_instance = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}' # format "adb-723483445396.18.azuredatabricks.net" (no https://) (don't change)
databricks_pat = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get() # format "dapi*****" (don't change)
username = spark.sql('select current_user() as user').collect()[0]['user'] # databricks user name
print(f"databricks instance: {databricks_instance}")
print(f"databricks personal access token: {databricks_pat}")
print(f"databricks username: {username}")

# COMMAND ----------

# DBTITLE 1,ETL Example Using Autoloader

# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
table_name = f"flight_tech_etl_quickstart_raw"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"
schema_name = "bronze"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
spark.sql(f"CREATE SCHEMA {schema_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(f"{schema_name}.{table_name}"))

# COMMAND ----------

# df = spark.read.table(table_name)

# COMMAND ----------

# display(df)
