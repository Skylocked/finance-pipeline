# Import modules
from pyspark.sql.functions import col, explode, to_json, from_json, current_timestamp
from pyspark.sql.types import MapType, StringType, DoubleType

# Ensure the target database schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS dev_finance.silver")


# Define path and table names
source_table = "dev_finance.bronze.exchange_rates"
target_table = "dev_finance.silver.exchange_rates"
checkpoint_path = "/Volumes/dev_finance/raw/remittance/_checkpoints/silver_exchange_rates"

# Print stdout message
print(f"Starting Silver transformation from: {source_table}")


# Read stream from bronze table
bronze_stream_df = spark.readStream.table("dev_finance.bronze.exchange_rates")


# Transform data from Bronze to Silver

# Spark's explode() requires a Dictionary (Map) or a List (Array). Auto Loader gave us an Object (Struct).
# We use a standard workaround to convert the Object into a Dictionary by quickly passing it through JSON.
rates_map = from_json(to_json(col("rates")), MapType(StringType(), DoubleType()))

# Add the new dictionary column to our dataframe
mapped_df = bronze_stream_df.withColumn("rates_dictionary", rates_map)

# Select the columns we want, cast the timestamp, and EXPLODE the dictionary into separate rows
silver_df = (
    mapped_df
    .select(
        col("base_code").alias("source_currency"),
        col("time_last_update_unix").cast("timestamp").alias("exchange_rate_timestamp"),
        explode(col("rates_dictionary")).alias("target_currency", "exchange_rate"),
        col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    )
   .withColumn("silver_processing_timestamp", current_timestamp())
   )


# Print stdout message for start of write stream into Delta table
print(f"Writing data to Silver Delta table: {target_table}")

# Write stream to Silver table
write_query = (silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .table(target_table)
)

# Wait for stream to finish micro-batching
write_query.awaitTermination()

# Print stdout message
print(f"Stream finished. Data written to table: {target_table}")