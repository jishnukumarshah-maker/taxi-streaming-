# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Hourly Zone Aggregations

# COMMAND ----------
from pyspark.sql.functions import window, count, sum as _sum, avg as _avg, round as _round, col

silver_df = spark.read.table("silver_trips")

gold_df = (
    silver_df.groupBy(
        col("pickup_location_id"),
        window(col("pickup_datetime"), "1 hour")
    )
    .agg(
        count("trip_id").alias("total_trips"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(_avg("trip_distance"), 2).alias("avg_distance")
    )
    .select(
        col("pickup_location_id"),
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("total_trips"),
        col("total_revenue"),
        col("avg_distance")
    )
)

gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_hourly_zone_stats")
