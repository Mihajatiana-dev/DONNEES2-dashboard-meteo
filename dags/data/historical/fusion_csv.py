import pandas as pd
import glob

# 1. Récupérer tous les fichiers CSV dans le dossier fichiers_csv
files = glob.glob("historical_data/*.csv")

# 2. Lire tous les fichiers et les stocker dans une liste
dfs = []
for file in files:
    df = pd.read_csv(file)
    dfs.append(df)

# 3. Fusionner tous les DataFrames
fusion = pd.concat(dfs, ignore_index=True)

# 4. Sauvegarder le résultat dans un nouveau fichier CSV
fusion.to_csv("historical.csv", index=False)

print("Fusion terminée ! Fichier : historical.csv")
