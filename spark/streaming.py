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
KAFKA_TOPIC = "orders"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce"
POSTGRES_TABLE = "orders"
POSTGRES_USER = "ecommerce"
POSTGRES_PASSWORD = "ecommerce"


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

order_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
])


# ---------------------------------------------------------
# Read events from Kafka
# ---------------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# ---------------------------------------------------------
# Convert Kafka JSON into structured columns
# ---------------------------------------------------------

orders_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(col("json_value"), order_schema).alias("order")
    )
    .select("order.*")
)


# ---------------------------------------------------------
# Write each micro-batch to PostgreSQL
# ---------------------------------------------------------

def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df
        .dropDuplicates(["event_id"])
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", POSTGRES_TABLE)
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
    orders_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/ecommerce-orders-checkpoint")
    .start()
)

query.awaitTermination()
