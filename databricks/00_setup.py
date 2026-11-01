# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Pipeline Initializer & Storage Setup

# COMMAND ----------
spark.sql("CREATE DATABASE IF NOT EXISTS nyc_taxi_lakehouse;")
spark.sql("USE nyc_taxi_lakehouse;")
print("Databricks lakehouse catalog initialized.")
