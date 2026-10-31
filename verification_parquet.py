from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FINANCE_BIGDATA_VERIFICATION")
    .getOrCreate()
)

try:
    df_valides = spark.read.parquet(
        "D:/FINANCE_BIGDATA/output/transactions_valides"
    )

    df_rejetees = spark.read.parquet(
        "D:/FINANCE_BIGDATA/output/transactions_rejetees"
    )

    print("TRANSACTIONS VALIDES")
    df_valides.show(truncate=False)

    print("TRANSACTIONS REJETEES")
    df_rejetees.show(truncate=False)
    nombre_valides = df_valides.count()
    nombre_rejetees = df_rejetees.count()
    nombre_total = nombre_valides + nombre_rejetees

    print("CONTROLE DES FICHIERS PARQUET")
    print("Transactions valides :", nombre_valides)
    print("Transactions rejetees :", nombre_rejetees)
    print("Total transactions :", nombre_total)

finally:
    spark.stop()