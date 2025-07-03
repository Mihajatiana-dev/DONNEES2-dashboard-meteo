import pandas as pd
import os
from datetime import datetime

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def merge_data(date: str) -> str:
    """
    Fusionne les nouvelles données avec l'historique (version optimisée)

    Améliorations :
    - Charge d'abord l'historique nettoyé si historical_weather.csv n'existe pas
    - Gère mieux les cas initiaux
    - Logs plus détaillés
    """
    # 1. Chargement des nouvelles données
    input_file = f"{BASE_PATH}/processed/cleaned_weather_{date}.csv"
    try:
        new_data = pd.read_csv(
            input_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={"ville": "category", "saison": "category"},
        )
        print(f"Chargement réussi des nouvelles données : {len(new_data)} lignes")

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
        raise ValueError(f"Erreur lecture {input_file} : {str(e)}")

    # 2. Chargement de l'historique (logique améliorée)
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
            f"Plage de dates : {merged_data['date_observation'].min()} à {merged_data['date_observation'].max()}"
        )
        return historical_file

    except Exception as e:
        raise ValueError(f"Erreur sauvegarde : {str(e)}")
