def transform_data(df_clients, df_comptes, df_transactions):
    """
    Enrichir les transactions avec les informations
    des comptes et des clients.
    """

    # 1. Relier les transactions aux comptes
    df_transaction_comptes = df_transactions.join(
        df_comptes.select("CompteID", "ClientID"),
        on="CompteID",
        how="left"
    )

    # 2. Relier les transactions aux clients
    df_final = df_transaction_comptes.join(
        df_clients.select(
            "ClientID",
            "NomClient",
            "Ville",
            "Segment"
        ),
        on="ClientID",
        how="left"
    )

    # 3. Retourner les données enrichies
    return df_final