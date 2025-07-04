import pandas as pd
import os
from datetime import datetime
import json
import logging

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def ensure_datetime(series):
    """Convertit une colonne en datetime de manière robuste"""
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    return pd.to_datetime(series, errors="coerce")


def generate_star_schema():
    """Transforme historical_weather.csv en modèle en étoile avec gestion robuste des types"""
    # Chemins des fichiers
    input_file = f"{BASE_PATH}/final/historical_weather.csv"
    output_dir = f"{BASE_PATH}/star_schema"
    metadata_file = f"{output_dir}/_metadata.json"
    os.makedirs(output_dir, exist_ok=True)

    # 1. Chargement avec typage strict
    try:
        weather_data = pd.read_csv(
            input_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={"ville": "string", "saison": "string", "conditions": "string"},
        )
        weather_data["date_observation"] = ensure_datetime(
            weather_data["date_observation"]
        )
    except Exception as e:
        logging.error(f"Erreur de chargement : {str(e)}")
        raise

    # 2. Gestion incrémentale
    try:
        with open(metadata_file) as f:
            metadata = json.load(f)
        last_date = pd.to_datetime(metadata["last_processed_date"])
        new_data = weather_data[weather_data["date_observation"] > last_date]
    except (FileNotFoundError, KeyError):
        metadata = {"schema_version": "1.0"}
        new_data = weather_data

    if new_data.empty:
        logging.info("Aucune nouvelle donnée à transformer")
        return

    # 3. Dimension Ville
    dim_cities_path = f"{output_dir}/dim_cities.csv"
    if os.path.exists(dim_cities_path):
        dim_cities = pd.read_csv(
            dim_cities_path, dtype={"city_id": "int64", "city_name": "string"}
        )
    else:
        dim_cities = pd.DataFrame(columns=["city_id", "city_name"])

    # Ajout nouvelles villes
    new_cities = set(new_data["ville"]) - set(dim_cities["city_name"])
    if new_cities:
        new_cities_df = pd.DataFrame(
            {
                "city_id": range(
                    len(dim_cities) + 1, len(dim_cities) + len(new_cities) + 1
                ),
                "city_name": list(new_cities),
            }
        )
        dim_cities = pd.concat([dim_cities, new_cities_df], ignore_index=True)
        dim_cities.to_csv(dim_cities_path, index=False)

    # 4. Dimension Temps (avec contrôle de type strict)
    dim_time_path = f"{output_dir}/dim_time.csv"
    time_cols = ["date_id", "full_date", "year", "month", "day", "season"]

    if os.path.exists(dim_time_path):
        dim_time = pd.read_csv(dim_time_path)
        dim_time["full_date"] = ensure_datetime(dim_time["full_date"])
    else:
        dim_time = pd.DataFrame(columns=time_cols)

    # Traitement des dates
    date_data = new_data[
        ["date_observation", "annee", "mois", "jour", "saison"]
    ].drop_duplicates()
    date_data = date_data.rename(
        columns={
            "date_observation": "full_date",
            "annee": "year",
            "mois": "month",
            "jour": "day",
            "saison": "season",
        }
    )
    date_data["full_date"] = ensure_datetime(date_data["full_date"])

    if not dim_time.empty:
        existing_dates = set(dim_time["full_date"])
        date_data = date_data[~date_data["full_date"].isin(existing_dates)]

    if not date_data.empty:
        date_data["date_id"] = range(
            len(dim_time) + 1, len(dim_time) + len(date_data) + 1
        )
        dim_time = pd.concat([dim_time, date_data[time_cols]], ignore_index=True)
        dim_time.to_csv(dim_time_path, index=False)

    # 5. Table de Faits (avec vérification des types)
    facts_path = f"{output_dir}/fact_weather.csv"

    # Jointure sécurisée
    try:
        facts = new_data.merge(
            dim_cities, left_on="ville", right_on="city_name", how="left"
        ).merge(
            dim_time,
            left_on=["date_observation", "annee", "mois", "jour", "saison"],
            right_on=["full_date", "year", "month", "day", "season"],
            how="left",
        )

        facts = facts.rename(
            columns={
                "temperature_moy": "avg_temp",
                "vent_moyen": "wind_speed",
                "visibilite_moy": "visibility",
            }
        )[
            [
                "city_id",
                "date_id",
                "avg_temp",
                "precipitation",
                "wind_speed",
                "visibility",
            ]
        ]

        # Sauvegarde incrémentale
        if os.path.exists(facts_path):
            existing_facts = pd.read_csv(facts_path)
            facts = pd.concat([existing_facts, facts], ignore_index=True)

        facts.to_csv(facts_path, index=False)

        # Mise à jour metadata
        metadata["last_processed_date"] = (
            weather_data["date_observation"].max().strftime("%Y-%m-%d")
        )
        with open(metadata_file, "w") as f:
            json.dump(metadata, f)

        logging.info(f"Star schema mis à jour. {len(new_data)} nouvelles lignes.")

    except Exception as e:
        logging.error(f"Erreur lors de la jointure : {str(e)}")
        raise
