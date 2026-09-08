import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_TOPICS = "orders,payments,shipments"



POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")  


# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("EcommerceOrderStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Kafka event schema
# ---------------------------------------------------------

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("shipment_status", StringType(), True),
])


# ---------------------------------------------------------
# Read events from Kafka
# ---------------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPICS)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ---------------------------------------------------------
# Convert Kafka JSON into structured columns
# ---------------------------------------------------------

events_df = (
    kafka_df
    .selectExpr("topic", "CAST(value AS STRING) AS json_value")
    .select(
        col("topic"),
        from_json(col("json_value"), event_schema).alias("event"),
    )
    .select("topic", "event.*")
)


# ---------------------------------------------------------
# Write each micro-batch to PostgreSQL
# ---------------------------------------------------------

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    table_columns = {
        "orders": [
            "event_id",
            "event_type",
            "event_time",
            "order_id",
            "customer_id",
            "customer_name",
            "product",
            "quantity",
            "amount",
            "country",
        ],
        "payments": [
            "event_id",
            "event_type",
            "event_time",
            "order_id",
            "customer_id",
            "amount",
            "payment_method",
            "payment_status",
        ],
        "shipments": [
            "event_id",
            "event_type",
            "event_time",
            "order_id",
            "customer_id",
            "carrier",
            "tracking_number",
            "shipment_status",
        ],
    }

    for topic, columns in table_columns.items():

        topic_df = (
            batch_df
            .filter(col("topic") == topic)
            .dropDuplicates(["event_id"])
            .select(*columns)
        )

        if topic_df.isEmpty():
            continue

        (
            topic_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", topic)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

    print(f"Processed batch: {batch_id}")


# ---------------------------------------------------------
# Start streaming query
# ---------------------------------------------------------

query = (
    events_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/ecommerce-events-checkpoint")
    .start()
)

query.awaitTermination()
