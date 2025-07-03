import pandas as pd
import os
from datetime import datetime
import glob

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def merge_data(date: str) -> str:
    """
    Fusionne TOUTES les nouvelles données avec l'historique (version optimisée)

    Améliorations :
    - Charge TOUS les fichiers cleaned_weather_*.csv existants (pas seulement celui du jour)
    - Charge d'abord l'historique nettoyé si historical_weather.csv n'existe pas
    - Gère mieux les cas initiaux
    - Logs plus détaillés
    """
    # 1. Chargement de TOUTES les nouvelles données
    processed_dir = f"{BASE_PATH}/processed"
    pattern = f"{processed_dir}/cleaned_weather_*.csv"
    
    try:
        # Trouver tous les fichiers cleaned_weather_*.csv
        files = glob.glob(pattern)
        if not files:
            raise ValueError(f"Aucun fichier cleaned_weather_*.csv trouvé dans {processed_dir}")
        
        print(f"Fichiers trouvés : {len(files)}")
        for file in files:
            print(f"  - {os.path.basename(file)}")
        
        # Charger tous les fichiers
        all_recent_data = []
        for file in files:
            try:
                df = pd.read_csv(
                    file,
                    parse_dates=["date_observation", "date_extraction"],
                    dtype={"ville": "category", "saison": "category"},
                )
                all_recent_data.append(df)
                print(f"Chargé {os.path.basename(file)} : {len(df)} lignes")
            except Exception as e:
                print(f"Erreur lecture {file} : {str(e)}")
                continue
        
        if not all_recent_data:
            raise ValueError("Aucun fichier de données récentes valide trouvé")
        
        # Concaténer toutes les données récentes
        new_data = pd.concat(all_recent_data, ignore_index=True)
        print(f"Total des données récentes chargées : {len(new_data)} lignes")
        
        # Vérification des colonnes critiques
        required_cols = {
            "ville",
            "date_observation",
            "temperature_moy",
            "precipitation",
            "vent_moyen",
            "score_meteo",
        }
        missing_cols = required_cols - set(new_data.columns)
        if missing_cols:
            raise ValueError(f"Colonnes manquantes : {missing_cols}")

    except Exception as e:
        raise ValueError(f"Erreur lecture des données récentes : {str(e)}")

    # 2. Chargement de l'historique (logique inchangée)
    historical_file = f"{BASE_PATH}/final/historical_weather.csv"
    historical_cleaned = f"{BASE_PATH}/historical/historical_cleaned.csv"

    try:
        # Cas 1: Fichier historique final existe déjà
        if os.path.exists(historical_file):
            historical_data = pd.read_csv(
                historical_file,
                parse_dates=["date_observation", "date_extraction"],
                dtype={"ville": "category", "saison": "category"},
            )
            print(
                f"Historique chargé depuis {historical_file} : {len(historical_data)} lignes"
            )

        # Cas 2: Premier run - utiliser historical_cleaned.csv
        elif os.path.exists(historical_cleaned):
            historical_data = pd.read_csv(
                historical_cleaned,
                parse_dates=["date_observation", "date_extraction"],
                dtype={"ville": "category", "saison": "category"},
            )
            print(
                f"Premier chargement historique depuis {historical_cleaned} : {len(historical_data)} lignes"
            )

        # Cas 3: Aucun historique disponible
        else:
            historical_data = pd.DataFrame()
            print("Aucun fichier historique trouvé, création d'un nouvel historique")

        # 3. Fusion et dédoublonnage
        merged_data = pd.concat([historical_data, new_data], ignore_index=True)
        merged_data = merged_data.sort_values("date_observation")

        # Garder la dernière version des doublons
        merged_data = merged_data.drop_duplicates(
            subset=["ville", "date_observation"], keep="last"
        )
        print(f"Après fusion et dédoublonnage : {len(merged_data)} lignes")
        
        # Statistiques pour vérification
        date_range = merged_data.groupby('ville')['date_observation'].agg(['min', 'max'])
        print("Plage de dates par ville :")
        print(date_range)

    except Exception as e:
        raise ValueError(f"Erreur lors de la fusion : {str(e)}")

    # 4. Sauvegarde optimisée
    os.makedirs(f"{BASE_PATH}/final", exist_ok=True)
    try:
        merged_data.to_csv(
            historical_file,
            index=False,
            date_format="%Y-%m-%d %H:%M:%S",
        )
        print(f"Sauvegarde réussie dans {historical_file}")
        print(
            f"Plage de dates globale : {merged_data['date_observation'].min()} à {merged_data['date_observation'].max()}"
        )
        return historical_file

    except Exception as e:
        raise ValueError(f"Erreur sauvegarde : {str(e)}")