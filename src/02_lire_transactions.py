from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import count
from pyspark.sql.functions import avg

# Démarrer Spark
spark = (
    SparkSession.builder
    .appName("FINANCE_BIGDATA_TRANSACTIONS")
    .master("local[*]")
    .getOrCreate()
)

# Lire le fichier CSV bancaire
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv("data/transactions.csv")
)

# Afficher les premières transactions
df.show(10, truncate=False)

# Afficher les colonnes et leurs types
df.printSchema()

# Calculer le montant total par type de transaction
#df.groupBy("TypeTransaction") \
 # .sum("Montant") \
  #.show()
# Convertir les montants en décimal
df = df.withColumn(
    "Montant",
    col("Montant").cast(DecimalType(18, 2))
)

# Calculer le montant total par type de transaction
df.groupBy("TypeTransaction") \
  .agg(spark_sum("Montant").alias("Total_Montant")) \
  .show()
  
  # Nombre de transactions par type
df.groupBy("TypeTransaction") \
  .agg(count("*").alias("Nombre_Transactions")) \
  .show()
  
  # Montant moyen par type de transaction
df.groupBy("TypeTransaction") \
  .agg(
      avg("Montant").cast(DecimalType(18, 2)).alias("Montant_Moyen")
  ) \
  .show()
  
  # Filtrer les transactions supérieures à 1500 TND
df.filter(
    col("Montant") > 1500
).show(20, truncate=False)

# Identifier les transactions rejetées
df.filter(
    col("Statut") == "Rejetee"
).show(20, truncate=False)

# Transactions rejetées
df_rejets = df.filter(col("Statut") == "Rejetee")

# Calculer les indicateurs
df_rejets.agg(
    count("*").alias("Nombre_Rejets"),
    spark_sum("Montant").alias("Montant_Total_Rejete")
).show()

# Lire les comptes bancaires
df_comptes = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv("data/comptes.csv")
)

# Afficher les comptes et leur structure
df_comptes.show(5, truncate=False)
df_comptes.printSchema()

# Jointure entre transactions et comptes
df_transactions_comptes = df.join(
    df_comptes.select(
        "CompteID",
        "ClientID",
        "TypeCompte"
    ),
    on="CompteID",
    how="left"
)

# Afficher les 10 premières lignes
df_transactions_comptes.show(10, truncate=False)

# Lire les clients bancaires
df_clients = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv("data/clients.csv")
)

# Vérifier les colonnes des clients
df_clients.show(5, truncate=False)
df_clients.printSchema()

# Lire les clients bancaires
df_clients = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ";")
    .csv("data/clients.csv")
)

# Vérifier les colonnes des clients
df_clients.show(5, truncate=False)
df_clients.printSchema()

# Jointure finale : transactions + comptes + clients
df_final = df_transactions_comptes.join(
    df_clients.select(
        "ClientID",
        "NomClient",
        "Ville",
        "Segment"
    ),
    on="ClientID",
    how="left"
)

# Afficher le résultat final
df_final.show(10, truncate=False)

#Vérifier la jointure finale
print("\nCONTROLE DE LA JOINTURE FINALE")

print("Transactions originales :", df.count())

print("Transactions après jointure :", df_final.count())

print(
    "Transactions sans client :",
    df_final.filter(df_final["NomClient"].isNull()).count()
)

# Exporter les données enrichies au format Parquet
df_final.write \
    .mode("overwrite") \
    .parquet("data/transactions_enrichies.parquet")

print("\nEXPORT PARQUET TERMINE")

# Lire les données enregistrées au format Parquet
df_parquet = spark.read.parquet(
    "data/transactions_enrichies.parquet"
)

print("\nVERIFICATION DU FICHIER PARQUET")

print("Nombre de transactions :", df_parquet.count())

print("Nombre de colonnes :", len(df_parquet.columns))

df_parquet.show(5, truncate=False)

from pyspark.sql.functions import count, sum as spark_sum

# Analyse des transactions par ville
df_analyse_ville = df_parquet.groupBy("Ville").agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nANALYSE BANCAIRE PAR VILLE")

df_analyse_ville.orderBy(
    "MontantTotal",
    ascending=False
).show(truncate=False)

# Analyse bancaire par segment
df_analyse_segment = df_parquet.groupBy("Segment").agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nANALYSE BANCAIRE PAR SEGMENT")

df_analyse_segment.orderBy(
    "MontantTotal",
    ascending=False
).show(truncate=False)

from pyspark.sql.functions import date_format

# Analyse mensuelle des transactions
df_analyse_mois = df_parquet.withColumn(
    "Mois",
    date_format("DateTransaction", "yyyy-MM")
).groupBy("Mois").agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nANALYSE BANCAIRE PAR MOIS")

df_analyse_mois.orderBy("Mois").show(
    truncate=False
)

# Analyse par statut des transactions
df_analyse_statut = df_parquet.groupBy("Statut").agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nANALYSE BANCAIRE PAR STATUT")

df_analyse_statut.show(truncate=False)

from pyspark.sql.functions import col, round as spark_round

# Calcul des KPI par statut
df_kpi_statut = df_analyse_statut.withColumn(
    "Pourcentage",
    spark_round(
        col("NombreTransactions") * 100 / df_parquet.count(),
        2
    )
)

print("\nKPI BANCAIRES PAR STATUT")

df_kpi_statut.orderBy(
    col("NombreTransactions").desc()
).show(truncate=False)

from pyspark.sql.functions import avg, round as spark_round

# Calcul du montant moyen par type de transaction
df_moyenne_type = df_parquet.groupBy("TypeTransaction").agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_round(
        avg("Montant"), 2
    ).alias("MontantMoyen")
)

print("\nMONTANT MOYEN PAR TYPE DE TRANSACTION")

df_moyenne_type.orderBy(
    "MontantMoyen",
    ascending=False
).show(truncate=False)

from pyspark.sql.functions import col

# Filtrer uniquement les transactions validées
df_transactions_validees = df_parquet.filter(
    col("Statut") == "Validee"
)

# Analyser les transactions validées par type
df_analyse_validees = df_transactions_validees.groupBy(
    "TypeTransaction"
).agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nANALYSE DES TRANSACTIONS VALIDEES")

df_analyse_validees.orderBy(
    "MontantTotal",
    ascending=False
).show(truncate=False)

# Détection des transactions de montant élevé
df_transactions_elevees = df_transactions_validees.filter(
    col("Montant") > 1500
)

print("\nTRANSACTIONS VALIDEES SUPERIEURES A 1500 TND")

df_transactions_elevees.select(
    "TransactionID",
    "NomClient",
    "Ville",
    "TypeTransaction",
    "Montant",
    "Statut"
).orderBy(
    col("Montant").desc()
).show(20, truncate=False)

# Analyse des transactions élevées par client
df_clients_eleves = df_transactions_elevees.groupBy(
    "ClientID",
    "NomClient"
).agg(
    count("TransactionID").alias("NombreTransactions"),
    spark_sum("Montant").alias("MontantTotal")
)

print("\nCLIENTS AVEC PLUSIEURS TRANSACTIONS ELEVEES")

df_clients_eleves.filter(
    col("NombreTransactions") >= 2
).orderBy(
    col("NombreTransactions").desc(),
    col("MontantTotal").desc()
).show(truncate=False)

from pyspark.sql.functions import when

# Attribution d'un niveau de risque
df_risque_clients = df_clients_eleves.withColumn(
    "NiveauRisque",
    when(
        col("NombreTransactions") >= 5,
        "Eleve"
    ).when(
        col("NombreTransactions") >= 3,
        "Moyen"
    ).otherwise("Faible")
)

print("\nCLASSIFICATION DES CLIENTS PAR RISQUE")

df_risque_clients.orderBy(
    col("NombreTransactions").desc()
).show(truncate=False)

from pyspark.sql.functions import countDistinct

# Synthèse par niveau de risque
df_synthese_risque = df_risque_clients.groupBy(
    "NiveauRisque"
).agg(
    countDistinct("ClientID").alias("NombreClients"),
    spark_sum("NombreTransactions").alias("NombreTransactions"),
    spark_sum("MontantTotal").alias("MontantTotal")
)

print("\nSYNTHESE DES RISQUES CLIENTS")

df_synthese_risque.orderBy(
    col("NombreTransactions").desc()
).show(truncate=False)

# Export du rapport des risques clients
df_synthese_risque.write.mode("overwrite").parquet(
    "data/rapport_risques_clients.parquet"
)

print("\nEXPORT DU RAPPORT DES RISQUES TERMINE")

# Vérification du fichier exporté
df_rapport = spark.read.parquet(
    "data/rapport_risques_clients.parquet"
)

print("\nVERIFICATION DU RAPPORT PARQUET")

df_rapport.show(truncate=False)

print("Nombre de lignes :", df_rapport.count())
# Fermer Spark
spark.stop()