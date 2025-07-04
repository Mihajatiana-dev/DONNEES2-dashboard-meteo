import pandas as pd
import os
from datetime import datetime
import glob
import logging

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def merge_data(date: str) -> str:
    """
    Fusionne les données selon l'approche optimisée :
    - Si historical_weather.csv n'existe pas : merge TOUS les fichiers cleaned + historical_cleaned.csv
    - S'il existe : merge uniquement le fichier du jour
    """
    historical_file = f"{BASE_PATH}/final/historical_weather.csv"
    daily_file = f"{BASE_PATH}/processed/cleaned_weather_{date}.csv"
    historical_cleaned = f"{BASE_PATH}/historical/historical_cleaned.csv"

    # 1. Chargement de l'historique ou initialisation
    if os.path.exists(historical_file):
        # Cas normal : charger l'historique existant
        historical_data = pd.read_csv(
            historical_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={"ville": "category", "saison": "category"},
        )
        logging.info(f"Historique chargé ({len(historical_data)} lignes)")

        # On ne charge que le fichier du jour
        new_data = pd.read_csv(
            daily_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={"ville": "category", "saison": "category"},
        )
        logging.info(f"Données du jour chargées ({len(new_data)} lignes)")

    else:
        # Cas initial : merge complet
        logging.info("Création nouvel historique - merge complet")

        # Charger historical_cleaned.csv si disponible
        historical_data = pd.DataFrame()
        if os.path.exists(historical_cleaned):
            historical_data = pd.read_csv(
                historical_cleaned,
                parse_dates=["date_observation", "date_extraction"],
                dtype={"ville": "category", "saison": "category"},
            )
            logging.info(f"Historique initial chargé ({len(historical_data)} lignes)")

        # Charger TOUS les fichiers cleaned
        all_files = glob.glob(f"{BASE_PATH}/processed/cleaned_weather_*.csv")
        if not all_files:
            logging.warning("Aucun fichier cleaned trouvé pour merge initial")

        new_data = pd.concat(
            [
                pd.read_csv(
                    f,
                    parse_dates=["date_observation", "date_extraction"],
                    dtype={"ville": "category", "saison": "category"},
                )
                for f in all_files
            ],
            ignore_index=True,
        )
        logging.info(
            f"{len(all_files)} fichiers cleaned mergés ({len(new_data)} lignes)"
        )

    # 2. Fusion et dédoublonnage
    merged_data = (
        pd.concat([historical_data, new_data])
        .drop_duplicates(subset=["ville", "date_observation"], keep="last")
        .sort_values("date_observation")
    )

    # 3. Vérification des données
    required_cols = {"ville", "date_observation", "temperature_moy", "precipitation"}
    if not required_cols.issubset(merged_data.columns):
        raise ValueError("Colonnes requises manquantes après merge")

    # 4. Sauvegarde
    os.makedirs(f"{BASE_PATH}/final", exist_ok=True)
    merged_data.to_csv(historical_file, index=False)

    logging.info(f"Merge final : {len(merged_data)} lignes")
    logging.info(
        f"Plage de dates : {merged_data['date_observation'].min()} à {merged_data['date_observation'].max()}"
    )

    return historical_file
