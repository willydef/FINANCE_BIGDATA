from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("FINANCE_BIGDATA_POWERBI") \
    .master("local[2]") \
    .getOrCreate()

os.makedirs("D:/FINANCE_BIGDATA/powerbi", exist_ok=True)

datasets = [
    "transactions_enrichies",
    "rapport_risques_clients"
]

try:
    for dataset in datasets:
        df = spark.read.parquet(
            f"D:/FINANCE_BIGDATA/data/{dataset}.parquet"
        )

        df.toPandas().to_csv(
            f"D:/FINANCE_BIGDATA/powerbi/{dataset}.csv",
            index=False,
            encoding="utf-8-sig"
        )

        print(f"EXPORT REUSSI : {dataset}.csv")

finally:
    spark.stop()