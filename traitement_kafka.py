import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    DoubleType, StringType
)

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FINANCE_BIGDATA_TRANSFORMATION")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
    )
    .getOrCreate()
)

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("montant", DoubleType(), True),
    StructField("type", StringType(), True),
    StructField("devise", StringType(), True)
])

try:
    df_kafka = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", "transactions_bancaires")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_transactions = (
        df_kafka
        .selectExpr("CAST(value AS STRING) AS json_transaction")
        .select(from_json(col("json_transaction"), schema).alias("data"))
        .select("data.*")
    )

    print("TRANSACTIONS BANCAIRES TRANSFORMEES")
    df_transactions.show(truncate=False)

finally:
    spark.stop()