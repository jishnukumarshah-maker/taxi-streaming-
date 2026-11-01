# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Publish Clean Data into Snowflake

# COMMAND ----------
sf_options = {
    "sfUrl": f"{dbutils.secrets.get('snowflake', 'account')}.snowflakecomputing.com",
    "sfUser": dbutils.secrets.get('snowflake', 'user'),
    "sfPassword": dbutils.secrets.get('snowflake', 'password'),
    "sfDatabase": "TAXI_DB",
    "sfSchema": "PROD",
    "sfWarehouse": "TAXI_WH"
}

# 1. Sync Gold Mart
spark.read.table("gold_hourly_zone_stats").write.format("snowflake").options(**sf_options).option("dbtable", "AGG_HOURLY_ZONE").mode("overwrite").save()

# 2. Stage Silver Facts
spark.read.table("silver_trips").write.format("snowflake").options(**sf_options).option("dbtable", "TAXI_DB.STAGING.STAGING_FACT_TRIPS").mode("overwrite").save()
