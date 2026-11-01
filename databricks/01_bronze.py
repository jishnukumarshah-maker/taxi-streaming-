# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Streaming Ingest from Confluent Cloud

# COMMAND ----------
try:
    KAFKA_BOOTSTRAP = dbutils.secrets.get(scope="kafka", key="bootstrap_server")
    KAFKA_KEY = dbutils.secrets.get(scope="kafka", key="api_key")
    KAFKA_SECRET = dbutils.secrets.get(scope="kafka", key="api_secret")
except Exception:
    dbutils.widgets.text("bootstrap_server", "")
    dbutils.widgets.text("api_key", "")
    dbutils.widgets.text("api_secret", "")
    KAFKA_BOOTSTRAP = dbutils.widgets.get("bootstrap_server")
    KAFKA_KEY = dbutils.widgets.get("api_key")
    KAFKA_SECRET = dbutils.widgets.get("api_secret")

jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{KAFKA_KEY}" password="{KAFKA_SECRET}";'
)

df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "trips.raw")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

(
    df_raw.selectExpr(
        "CAST(key AS STRING) AS location_key",
        "CAST(value AS STRING) AS raw_payload",
        "topic", "partition", "offset",
        "timestamp AS kafka_timestamp",
        "current_timestamp() AS bronze_ingested_at"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/confluent_bronze")
    .trigger(availableNow=True)
    .toTable("bronze_trips")
).awaitTermination()
