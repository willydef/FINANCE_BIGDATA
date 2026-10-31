from pyspark.sql import SparkSession

from extract import extract_data
from transform import transform_data
from load import load_data


def main():
    # 1. Initialiser Spark
    spark = SparkSession.builder \
        .appName("FINANCE_BIGDATA_ETL") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # 2. EXTRACTION
        print("\nETAPE 1 : EXTRACTION")

        df_clients, df_comptes, df_transactions = extract_data(spark)

        print("Clients :", df_clients.count())
        print("Comptes :", df_comptes.count())
        print("Transactions :", df_transactions.count())

        # 3. TRANSFORMATION
        print("\nETAPE 2 : TRANSFORMATION")

        df_final = transform_data(
            df_clients,
            df_comptes,
            df_transactions
        )

        print("Transactions enrichies :", df_final.count())

        # 4. CHARGEMENT
        print("\nETAPE 3 : CHARGEMENT")

        load_data(
            df_final,
            "data/etl_transactions.parquet"
        )
        
        # 5. Vérification après chargement
        print("\nETAPE 4 : VALIDATION DU CHARGEMENT")

        df_verification = spark.read.parquet(
            "data/etl_transactions.parquet"
        )

        print("Lignes enregistrées :", df_verification.count())
                # Contrôle qualité du pipeline
        nombre_source = df_transactions.count()
        nombre_final = df_verification.count()

        assert nombre_source == nombre_final, (
            f"ERREUR : {nombre_source} transactions en entrée, "
            f"mais {nombre_final} transactions en sortie"
        )

        print("CONTROLE QUALITE : OK")
        print("Transactions source :", nombre_source)
        print("Transactions finales :", nombre_final)

        print("\nSCHEMA DES DONNEES")
        df_verification.printSchema()

        print("\nAPERÇU DES DONNEES")
        df_verification.show(5, truncate=False)

                # CONTROLE DES DOUBLONS
        print("\nCONTROLE DES DOUBLONS")

        nombre_total = df_verification.count()

        nombre_unique = df_verification.select(
            "TransactionID"
        ).distinct().count()

        nombre_doublons = nombre_total - nombre_unique

        print("Transactions totales :", nombre_total)
        print("TransactionID uniques :", nombre_unique)
        print("Doublons détectés :", nombre_doublons)

        assert nombre_doublons == 0, (
            f"ERREUR : {nombre_doublons} doublons détectés"
        )

        print("CONTROLE DOUBLONS : OK")
        
                # CONTROLE DES VALEURS NULL
        print("\nCONTROLE DES VALEURS NULL")

        colonnes_obligatoires = [
            "TransactionID",
            "CompteID",
            "ClientID",
            "Montant",
            "NomClient"
        ]

        for colonne in colonnes_obligatoires:
            nombre_null = df_verification.filter(
                df_verification[colonne].isNull()
            ).count()

            print(f"{colonne} : {nombre_null} NULL")

            assert nombre_null == 0, (
                f"ERREUR : {nombre_null} valeurs NULL dans {colonne}"
            )

        print("CONTROLE NULL : OK")
        
                # CONTROLE DES MONTANTS INVALIDES
        print("\nCONTROLE DES MONTANTS INVALIDES")

        df_montants_invalides = df_verification.filter(
            df_verification["Montant"] <= 0
        )

        nombre_invalides = df_montants_invalides.count()

        print("Montants invalides :", nombre_invalides)

        assert nombre_invalides == 0, (
            f"ERREUR : {nombre_invalides} montants invalides"
        )

        print("CONTROLE MONTANTS : OK")
        print("\nPIPELINE ETL TERMINE AVEC SUCCES")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
   