# Import modules
import sys          # parameterization
from pyspark.sql.functions import current_timestamp, col

# No need to import from pyspark.sql
# In Databricks, you do not need to build or initialize a SparkSession as it automatically builds one for you the exact moment your compute cluster boots up, 
# and it injects it directly into your notebooks as a globally available variable named spark

#   from pyspark.sql import SparkSession
#   my_spark = SparkSession.builder.appName("my_spark").getOrCreate()
#   print(my_spark)

# 1st my_spark is variable name inside python (spark is the default name in databricks)
# 2nd my_spark is the spark cluster or engine name (used to track session in spark engine)


# Catch the parameter (and ignore IPython's interactive -f flag)
if len(sys.argv) > 1 and not sys.argv[1].startswith("-f"): 
    env_catalog = sys.argv[1]
    print(f"Running in Job mode. Target catalog: {env_catalog}")
else:
    env_catalog = "dev_finance"
    print(f"Running in Local Dev mode. Defaulting to catalog: {env_catalog}")


# Define paths and table names
source_volume_path = f"/Volumes/{env_catalog}/raw/remittance/"                                     # Where our raw JSON is stored
target_table = f"{env_catalog}.bronze.exchange_rates"                                              # Where the target database table is
checkpoint_path = f"/Volumes/{env_catalog}/raw/remittance/_checkpoints/bronze_exchange_rates"      # Where Auto Loader will save its tracking/memory files


# Print stdout message
print(f"Starting Auto Loader ingestion from: {source_volume_path}")


# Read the raw JSON files as a stream using Auto Loader (cloudFiles)
raw_stream_df = (
    spark.readStream
    .format("cloudFiles")                                   # Use Databricks Auto Loader engine
    .option("cloudFiles.format", "json")                    # Reading JSON files
    .option("cloudFiles.inferColumnTypes", "true")          # Automatically guess the data types (string, int, double)
    .option("cloudFiles.schemaLocation", checkpoint_path)   # Where to save the guessed schema so it remembers for next time

    # Force auto loader to re-scan the volume for already processed files in case reupload is necessary
    # .option("cloudFiles.backfillInterval", "1 day") # Forces a full scan of files from the last 24 hours
    
    .load(source_volume_path)                               # The folder to observe for new files
)


# spark.read returns DataFrameReader object used for batch
#  Processes a static, bounded snapshot of data that has clear beginning and end

# spark.readStream returns DataStreamReader object used for streaming
# sets up  a continuous unbounded listere that waits for new files or messages to arrive indefinitely

# Add standard data engineering audit columns to our streaming DataFrame (timesteamp of insert + source table)
# source_file commented out to bypass the Unity Catalog hidden column bug)
bronze_df = (
    raw_stream_df
    .withColumn("ingestion_timestamp", current_timestamp())     # Adds column with data on loading time
    # .withColumn("source_file", col('"_metadata.file_path'))     # Adds column with data on original filename source
)


# Print stdout message for start of write stream into Delta table
print(f"Writing data to Bronze Delta table: {target_table}")


# Write the stream into a Delta Table 
write_query = (bronze_df.writeStream
    .format("delta")                               # Save as Delta Lake table
    .outputMode("append")                          # Only add new rows, do not overwrite existing ones
    .option("checkpointLocation", checkpoint_path) # Tracks exactly which files have already been processed
    .trigger(availableNow=True)                    # Process all outstanding files and stop compute
    .table(target_table)                           # Save into target destination
)


# Wait for the stream to finish its micro-batch
write_query.awaitTermination()


# print stdout message
print(f"Stream finished. Data written to table: {target_table}")
