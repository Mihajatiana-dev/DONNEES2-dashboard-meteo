import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def extract_historical_data(output_path: str) -> bool:
    """Convertit historical.csv vers le format brut des données récentes"""
    try:
        df = pd.read_csv(f"{BASE_PATH}/historical/historical.csv")

        # Conversion des colonnes historiques -> format récent
        df_converted = pd.DataFrame(
            {
                "ville": df["name"],
                "date_extraction": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),  # Date de conversion
                "date_observation": pd.to_datetime(df["datetime"])
                + pd.Timedelta(hours=12),  # Ajout de l'heure
                "temperature_moy": df["temp"],  # temp = moyenne dans l'historique
                "temp_min": df["tempmin"],
                "temp_max": df["tempmax"],
                "humidite_moy": df["humidity"].round().astype(int),
                "precipitation": df["precip"],  # en mm
                "vent_moyen": df["windspeed"],  # en km/h
                "visibilite_moy": df["visibility"],  # en km
                "conditions": df["conditions"]
                .str.split(",")
                .str[0],  # "Partially cloudy" -> "Partially cloudy"
                "nb_intervalles": 24,  # Valeur par défaut pour l'historique
            }
        )

        # Sauvegarde dans un seul fichier (pas de sous-dossiers par date)
        df_converted.to_csv(output_path, index=False)
        logging.info(f"Données historiques converties sauvegardées dans {output_path}")
        return True

    except Exception as e:
        logging.error(f"Erreur conversion historique : {str(e)}", exc_info=True)
        return False


def extract_weather_data(city: str, api_key: str, date: str) -> bool:
    """
    Extrait les données météo pour une ville

    - Utilisation de l'API Forecast pour des prévisions sur 24h
    - Calcul des cumuls de précipitations journalières
    - Récupération des températures min/max/moyenne sur 24h

    Args:
        city (str): Nom de la ville
        api_key (str): Clé API OpenWeather
        date (str): Date au format 'YYYY-MM-DD'

    Returns:
        bool: True si succès, False sinon
    """
    try:
        # 1. Récupération des coordonnées
        geo_url = "http://api.openweathermap.org/geo/1.0/direct"
        geo_params = {"q": city, "limit": 1, "appid": api_key}
        geo_response = requests.get(geo_url, params=geo_params, timeout=10)
        geo_response.raise_for_status()
        geo_data = geo_response.json()[0]
        lat, lon = geo_data["lat"], geo_data["lon"]

        # 2. Appel à l'API Forecast
        forecast_url = "http://api.openweathermap.org/data/2.5/forecast"
        forecast_params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": "metric",
            "cnt": 24,  # Prévisions sur 24h (8 intervalles de 3h)
        }
        forecast_response = requests.get(
            forecast_url, params=forecast_params, timeout=15
        )
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()

        # 3. Calcul des indicateurs sur 24h
        precipitations = []
        temperatures = []
        humidites = []
        vents = []
        visibilites = []

        for interval in forecast_data["list"]:
            precipitations.append(interval.get("rain", {}).get("3h", 0))
            temperatures.append(interval["main"]["temp"])
            humidites.append(interval["main"]["humidity"])
            vents.append(interval["wind"]["speed"])
            visibilites.append(interval.get("visibility", 10000) / 1000)

        # 4. Structuration des données améliorée
        combined_data = {
            "ville": city,
            "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date_observation": date
            + " 12:00:00",  # Heure fixe pour l'agrégation journalière
            "temperature_moy": round(sum(temperatures) / len(temperatures), 1),
            "temp_min": min(temperatures),
            "temp_max": max(temperatures),
            "humidite_moy": round(sum(humidites) / len(humidites)),
            "precipitation": round(sum(precipitations), 1),  # Cumul sur 24h en mm
            "vent_moyen": round(sum(vents) / len(vents), 1),
            "visibilite_moy": round(sum(visibilites) / len(visibilites), 1),
            "conditions": forecast_data["list"][0]["weather"][0][
                "description"
            ],  # Conditions actuelles
            "nb_intervalles": len(forecast_data["list"]),  # Pour vérification
        }

        # 5. Sauvegarde
        output_dir = f"{BASE_PATH}/raw/{date}"
        os.makedirs(output_dir, exist_ok=True)
        pd.DataFrame([combined_data]).to_csv(
            f"{output_dir}/weather_{city}.csv", index=False
        )

        # Log de vérification
        logging.info(f"Données {city} extraites : {combined_data}")
        return True

    except Exception as e:
        logging.error(f"Erreur pour {city}: {str(e)}", exc_info=True)
        return False
