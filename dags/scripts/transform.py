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
    """Transforme historical_weather.csv en modèle en étoile simplifié"""
    # Chemins des fichiers
    input_file = f"{BASE_PATH}/final/historical_weather.csv"
    output_dir = f"{BASE_PATH}/final/star_schema"
    metadata_file = f"{output_dir}/_metadata.json"
    os.makedirs(output_dir, exist_ok=True)

    # 1. Chargement avec typage strict
    try:
        weather_data = pd.read_csv(
            input_file,
            parse_dates=["date_observation", "date_extraction"],
            dtype={
                "ville": "string",
                "saison": "string",
                "conditions": "string",
                "temp_ideale": "bool",
                "peu_vent": "bool",
                "peu_pluie": "bool",
                "bonne_visibilite": "bool",
                "periode_recommandee": "bool",
            },
        )
        weather_data["date_observation"] = ensure_datetime(
            weather_data["date_observation"]
        )
    except Exception as e:
        logging.error(f"Erreur de chargement : {str(e)}")
        raise

    # 2. Gestion incrémentale (inchangée)
    try:
        with open(metadata_file) as f:
            metadata = json.load(f)
        last_date = pd.to_datetime(metadata["last_processed_date"])
        new_data = weather_data[weather_data["date_observation"] > last_date]
    except (FileNotFoundError, KeyError):
        metadata = {"schema_version": "1.1"}  # Version incrémentée
        new_data = weather_data

    if new_data.empty:
        logging.info("Aucune nouvelle donnée à transformer")
        return

    # 3. Dimension Ville (simplifiée)
    dim_city_path = f"{output_dir}/dim_city.csv"
    if os.path.exists(dim_city_path):
        dim_city = pd.read_csv(
            dim_city_path, dtype={"city_id": "int64", "city_name": "string"}
        )
    else:
        dim_city = pd.DataFrame(columns=["city_id", "city_name"])

    new_cities = set(new_data["ville"]) - set(dim_city["city_name"])
    if new_cities:
        new_city_df = pd.DataFrame(
            {
                "city_id": range(
                    len(dim_city) + 1, len(dim_city) + len(new_cities) + 1
                ),
                "city_name": list(new_cities),
            }
        )
        dim_city = pd.concat([dim_city, new_city_df], ignore_index=True)
        dim_city.to_csv(dim_city_path, index=False)

    # 4. Dimension Date (optimisée)
    dim_date_path = f"{output_dir}/dim_date.csv"
    date_cols = ["date_id", "full_date", "year", "month", "day", "season"]

    if os.path.exists(dim_date_path):
        dim_date = pd.read_csv(dim_date_path, parse_dates=["full_date"])
    else:
        dim_date = pd.DataFrame(columns=date_cols)

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

    if not dim_date.empty:
        existing_dates = set(dim_date["full_date"])
        date_data = date_data[~date_data["full_date"].isin(existing_dates)]

    if not date_data.empty:
        date_data["date_id"] = range(
            len(dim_date) + 1, len(dim_date) + len(date_data) + 1
        )
        dim_date = pd.concat([dim_date, date_data[date_cols]], ignore_index=True)
        dim_date.to_csv(dim_date_path, index=False)

    # 5. Nouvelle Dimension Conditions
    dim_conditions_path = f"{output_dir}/dim_conditions.csv"
    if os.path.exists(dim_conditions_path):
        dim_conditions = pd.read_csv(
            dim_conditions_path,
            dtype={"condition_id": "int64", "condition_name": "string"},
        )
    else:
        dim_conditions = pd.DataFrame(columns=["condition_id", "condition_name"])

    new_conditions = set(new_data["conditions"]) - set(dim_conditions["condition_name"])
    if new_conditions:
        new_conditions_df = pd.DataFrame(
            {
                "condition_id": range(
                    len(dim_conditions) + 1,
                    len(dim_conditions) + len(new_conditions) + 1,
                ),
                "condition_name": list(new_conditions),
            }
        )
        dim_conditions = pd.concat(
            [dim_conditions, new_conditions_df], ignore_index=True
        )
        dim_conditions.to_csv(dim_conditions_path, index=False)

    # 6. Table de Faits complète
    facts_path = f"{output_dir}/fact_weather.csv"

    try:
        # Jointure avec toutes les dimensions
        facts = (
            new_data.merge(dim_city, left_on="ville", right_on="city_name")
            .merge(
                dim_date,
                left_on=["date_observation", "annee", "mois", "jour", "saison"],
                right_on=["full_date", "year", "month", "day", "season"],
            )
            .merge(dim_conditions, left_on="conditions", right_on="condition_name")[
                [
                    "city_id",
                    "date_id",
                    "condition_id",
                    "temperature_moy",
                    "temp_min",
                    "temp_max",
                    "humidite_moy",
                    "precipitation",
                    "vent_moyen",
                    "visibilite_moy",
                    "temp_ideale",
                    "peu_vent",
                    "peu_pluie",
                    "bonne_visibilite",
                    "score_meteo",
                    "periode_recommandee",
                ]
            ]
            .rename(
                columns={
                    "temperature_moy": "avg_temp",
                    "temp_min": "min_temp",
                    "temp_max": "max_temp",
                    "humidite_moy": "humidity",
                    "vent_moyen": "wind_speed",
                    "visibilite_moy": "visibility",
                }
            )
        )

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

        logging.info(
            f"Star schema mis à jour. {len(new_data)} nouvelles lignes. Modèle v1.1"
        )

    except Exception as e:
        logging.error(f"Erreur lors de la jointure : {str(e)}")
        raise
