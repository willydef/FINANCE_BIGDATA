def load_data(df, output_path):
    """
    Enregistrer un DataFrame PySpark au format Parquet.
    """

    df.write.mode("overwrite").parquet(output_path)

    print(f"Export Parquet terminé : {output_path}")