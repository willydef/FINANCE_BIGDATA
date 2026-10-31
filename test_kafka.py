import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FINANCE_BIGDATA_KAFKA")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"
    )
    .getOrCreate()
)

try:
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", "transactions_bancaires")
        .option("startingOffsets", "earliest")
        .load()
    )

    df.selectExpr("CAST(value AS STRING) AS transaction").show(
        truncate=False
    )

finally:
    spark.stop()