from pyspark.sql.types import DecimalType


def extract_data(spark):
    """
    Extraction des données bancaires depuis les CSV.
    """

    df_clients = spark.read.csv(
        "data/clients.csv",
        header=True,
        inferSchema=True,
        sep=";"
    )

    df_comptes = spark.read.csv(
        "data/comptes.csv",
        header=True,
        inferSchema=True,
        sep=";"
    )

    df_transactions = spark.read.csv(
        "data/transactions.csv",
        header=True,
        inferSchema=True,
        sep=";"
    )

    # Conversion du montant en type décimal
    df_transactions = df_transactions.withColumn(
        "Montant",
        df_transactions["Montant"].cast(DecimalType(18, 2))
    )

    return df_clients, df_comptes, df_transactions