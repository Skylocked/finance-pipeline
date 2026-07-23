# Import modules
import sys          # parameterization
# No need to import spark because we are using the base Spark SQL module which is pre-imported for us


# Catch the parameter (and ignore IPython's interactive -f flag)
if len(sys.argv) > 1 and not sys.argv[1].startswith("-f"): 
    env_catalog = sys.argv[1]
    print(f"Running in Job mode. Target catalog: {env_catalog}")
else:
    env_catalog = "dev_finance"
    print(f"Running in Local Dev mode. Defaulting to catalog: {env_catalog}")


# Ensure target database schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {env_catalog}.gold")


# Define the path and table names
source_table = f"{env_catalog}.silver.exchange_rates"
target_table = f"{env_catalog}.gold.exchange_rates"


# Print stdout message
print(f"Starting Gold transformations from: {source_table}")


# Transform data using Spark SQL with DWH approach
gold_df = spark.sql(f"""
  WITH base AS (
    SELECT CAST(exchange_rate_timestamp AS DATE) as exchange_date
      , source_currency
      , target_currency
      , exchange_rate
      , ROW_NUMBER() OVER(PARTITION BY source_currency, target_currency, CAST(exchange_rate_timestamp AS DATE) ORDER BY exchange_rate_timestamp) as rn
    FROM {source_table}
  )
  , deduplicated AS (
    SELECT exchange_date
        , source_currency
        , target_currency
        , exchange_rate
        , LAG(exchange_rate, 1) OVER (PARTITION BY source_currency, target_currency ORDER BY exchange_date) AS previous_day_rate
    FROM base
    WHERE rn = 1
  )
  SELECT exchange_date
      , source_currency
      , target_currency
      , ROUND(exchange_rate, 4) as exchange_rate
      , ROUND(previous_day_rate, 4) as previous_day_rate
      , ROUND(exchange_rate - previous_day_rate, 4) as rate_variance
  FROM deduplicated
  ORDER BY exchange_date DESC
    , target_currency
""")


# Print stdout
print(f"Writing to: {target_table}")


# Write data to target table
(gold_df.write
    .format("delta")                        # use delta format
    .mode("overwrite")                      # overwrite table with freshly calculated aggregates
    .option("overwriteSchema", "true")      # anticipation of schema evolution
    .saveAsTable(target_table)
)


# Print stdout message
print(f"Write finished. Data written to table: {target_table}")