from pyspark.sql import SparkSession

# 1. Créer une session Spark
spark = (
    SparkSession.builder
    .appName("FINANCE_BIGDATA")
    .master("local[*]")
    .getOrCreate()
)

# 2. Créer des transactions bancaires fictives
transactions = [
    (1, "Client_A", 1500.00),
    (2, "Client_B", 2500.00),
    (3, "Client_C", 800.00),
]

# 3. Transformer les données en DataFrame Spark
df = spark.createDataFrame(
    transactions,
    ["ID_TRANSACTION", "CLIENT", "MONTANT"]
)

# 4. Afficher les transactions
df.show()

# 5. Fermer Spark
spark.stop()