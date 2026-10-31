import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
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
    print("VALIDATION DES TRANSACTIONS BANCAIRES")

    df_valides = df_transactions.filter(
        col("transaction_id").isNotNull()
        & col("client_id").isNotNull()
        & col("montant").isNotNull()
        & (col("montant") > 0)
    )

    print("TRANSACTIONS VALIDES")
    df_valides.show(truncate=False)
    total_transactions = df_transactions.count()
    total_valides = df_valides.count()
    total_rejetees = total_transactions - total_valides

    print("RAPPORT DE VALIDATION")
    print("Total transactions :", total_transactions)
    print("Transactions valides :", total_valides)
    print("Transactions rejetees :", total_rejetees)
    print("DETAIL DES TRANSACTIONS REJETEES")

    df_rejetees = df_transactions.subtract(df_valides)

    df_rejetees = df_rejetees.withColumn(
        "motif_rejet",
        when(col("transaction_id").isNull(), lit("ID transaction manquant"))
        .when(col("client_id").isNull(), lit("ID client manquant"))
        .when(col("montant").isNull(), lit("Montant manquant"))
        .when(col("montant") <= 0, lit("Montant invalide"))
        .otherwise(lit("Autre erreur"))
    )

    df_rejetees.show(truncate=False)
    print("ENREGISTREMENT DES TRANSACTIONS EN PARQUET")

    df_valides.write.mode("overwrite").parquet(
        "D:/FINANCE_BIGDATA/output/transactions_valides"
    )

    df_rejetees.write.mode("overwrite").parquet(
        "D:/FINANCE_BIGDATA/output/transactions_rejetees"
    )

    print("Fichiers Parquet enregistres avec succes")

finally:
    spark.stop()