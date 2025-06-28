import pandas as pd
import os

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def merge_data(date: str) -> str:
    # 1. Chargement des nouvelles données
    input_file = f"{BASE_PATH}/processed/cleaned_weather_air_{date}.csv"
    new_data = pd.read_csv(input_file)

    # 2. Chargement de l'historique
    historical_file = f"{BASE_PATH}/final/historical_weather_air.csv"
    if os.path.exists(historical_file):
        historical_data = pd.read_csv(historical_file)
        merged_data = pd.concat([historical_data, new_data]).drop_duplicates(
            subset=["ville", "date_observation"], keep="last"
        )
    else:
        merged_data = new_data

    # 3. Sauvegarde
    os.makedirs(f"{BASE_PATH}/final", exist_ok=True)
    merged_data.to_csv(historical_file, index=False)

    return historical_file
