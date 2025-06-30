import pandas as pd
import os
from datetime import datetime

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def merge_data(date: str) -> str:
    """
    Fusionne les nouvelles données avec l'historique (version optimisée)

    Améliorations :
    - Vérification renforcée des colonnes
    - Conversion cohérente des dates
    - Optimisation mémoire pour les gros fichiers
    """
    # 1. Chargement des nouvelles données
    input_file = f"{BASE_PATH}/processed/cleaned_weather_{date}.csv"
    try:
        new_data = pd.read_csv(
            input_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={"ville": "category", "saison": "category"},  # Optimisation mémoire
        )

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

    # 2. Chargement de l'historique
    historical_file = f"{BASE_PATH}/final/historical_weather.csv"
    try:
        if os.path.exists(historical_file):
            historical_data = pd.read_csv(
                historical_file,
                parse_dates=["date_observation", "date_extraction"],
                dtype={"ville": "category", "saison": "category"},
            )

            # Fusion intelligente
            merged_data = pd.concat([historical_data, new_data], ignore_index=True)
            merged_data = merged_data.sort_values("date_observation")

            # Dédoublonnage (garder la dernière version)
            merged_data = merged_data.drop_duplicates(
                subset=["ville", "date_observation"], keep="last"
            )
        else:
            merged_data = new_data

    except Exception as e:
        raise ValueError(f"Erreur fusion historique : {str(e)}")

    # 3. Sauvegarde optimisée
    os.makedirs(f"{BASE_PATH}/final", exist_ok=True)
    try:
        merged_data.to_csv(
            historical_file,
            index=False,
            date_format="%Y-%m-%d %H:%M:%S",  # Format cohérent
        )
        print(f"Fusion réussie. Taille historique : {len(merged_data)} lignes")
        return historical_file

    except Exception as e:
        raise ValueError(f"Erreur sauvegarde : {str(e)}")
