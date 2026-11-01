# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Processing & Quarantine Routing (9 Data Quality Rules)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col, when, to_timestamp

schema = StructType([
    StructField("trip_id", StringType(), False),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("payment_type", IntegerType(), True)
])

bronze_df = spark.read.table("bronze_trips")
parsed_df = bronze_df.select(from_json(col("raw_payload"), schema).alias("d")).select("d.*")

flagged_df = parsed_df.withColumn(
    "quarantine_reason",
    when(col("trip_id").isNull(), "MISSING_TRIP_ID")
    .when((col("passenger_count") < 1) | (col("passenger_count") > 8), "INVALID_PASSENGER_COUNT")
    .when((col("trip_distance") <= 0.0) | (col("trip_distance") > 150.0), "INVALID_DISTANCE")
    .when((col("fare_amount") <= 0.0) | (col("fare_amount") > 2000.0), "INVALID_FARE")
    .when(col("total_amount") < col("fare_amount"), "TOTAL_LESS_THAN_FARE")
    .when(col("pickup_datetime") >= col("dropoff_datetime"), "INVALID_TIMESTAMPS")
    .when(col("pickup_location_id").isNull() | col("dropoff_location_id").isNull(), "NULL_LOCATION")
    .when(~col("payment_type").isin([1, 2, 3, 4, 5, 6]), "UNKNOWN_PAYMENT")
    .when((col("trip_distance") == 0.0) & (col("fare_amount") > 20.0), "ZERO_DIST_HIGH_FARE")
    .otherwise("VALID")
)

clean_df = (
    flagged_df.filter(col("quarantine_reason") == "VALID")
    .drop("quarantine_reason")
    .withColumn("pickup_datetime", to_timestamp("pickup_datetime"))
    .withColumn("dropoff_datetime", to_timestamp("dropoff_datetime"))
    .dropDuplicates(["trip_id"])
)

quarantine_df = (
    flagged_df.filter(col("quarantine_reason") != "VALID")
    .withColumn("quarantined_at", col("current_timestamp()"))
)

clean_df.write.format("delta").mode("append").saveAsTable("silver_trips")
quarantine_df.write.format("delta").mode("append").saveAsTable("quarantine_trips")
