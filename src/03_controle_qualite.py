import pandas as pd

# Lire les trois fichiers CSV
clients = pd.read_csv("data/clients.csv", sep=";")
comptes = pd.read_csv("data/comptes.csv", sep=";")
transactions = pd.read_csv("data/transactions.csv", sep=";")

# Vérifier le nombre de lignes et de colonnes
print("CLIENTS :", clients.shape)
print("COMPTES :", comptes.shape)
print("TRANSACTIONS :", transactions.shape)

# Afficher les premières lignes
print("\nCLIENTS")
print(clients.head())

print("\nCOMPTES")
print(comptes.head())

print("\nTRANSACTIONS")
print(transactions.head())

# Vérifier les valeurs manquantes dans chaque fichier
print("\nVALEURS MANQUANTES - CLIENTS")
print(clients.isnull().sum())

print("\nVALEURS MANQUANTES - COMPTES")
print(comptes.isnull().sum())

print("\nVALEURS MANQUANTES - TRANSACTIONS")
print(transactions.isnull().sum())

#érifier qu'aucun identifiant bancaire n'apparaît plusieurs fois alors qu'il devrait être unique.
# Vérification des doublons sur les clés primaires

print("\nDOUBLONS - CLIENTS")
print(clients["ClientID"].duplicated().sum())

print("\nDOUBLONS - COMPTES")
print(comptes["CompteID"].duplicated().sum())

print("\nDOUBLONS - TRANSACTIONS")
print(transactions["TransactionID"].duplicated().sum())

#Vérifier les dates bancaires
# Vérifier les formats de dates

dates_clients = pd.to_datetime(
    clients["DateOuverture"],
    format="%Y-%m-%d",
    errors="coerce"
)

dates_transactions = pd.to_datetime(
    transactions["DateTransaction"],
    format="%Y-%m-%d",
    errors="coerce"
)

print("\nDATES INVALIDES - CLIENTS")
print(dates_clients.isna().sum())

print("\nDATES INVALIDES - TRANSACTIONS")
print(dates_transactions.isna().sum())

#Vérifier les montants bancaire
#détecter les montants négatifs, nuls ou non numériques
# Vérifier les montants des transactions
montants = pd.to_numeric(
    transactions["Montant"],
    errors="coerce"
)

print("\nMONTANTS NON NUMERIQUES")
print(montants.isna().sum())

print("\nMONTANTS NEGATIFS")
print((montants < 0).sum())

print("\nMONTANTS NULS")
print((montants == 0).sum())

# Vérifier les soldes des comptes
soldes = pd.to_numeric(
    comptes["Solde"],
    errors="coerce"
)

print("\nSOLDES NON NUMERIQUES")
print(soldes.isna().sum())

#détecter les comptes associés à des clients inexistants
#et les transactions associées à des comptes inexistants.
# Vérifier que chaque compte appartient à un client existant
comptes_sans_client = comptes[
    ~comptes["ClientID"].isin(clients["ClientID"])
]

# Vérifier que chaque transaction possède un compte existant
transactions_sans_compte = transactions[
    ~transactions["CompteID"].isin(comptes["CompteID"])
]

print("\nCOMPTES SANS CLIENT")
print(len(comptes_sans_client))

print("\nTRANSACTIONS SANS COMPTE")
print(len(transactions_sans_compte))