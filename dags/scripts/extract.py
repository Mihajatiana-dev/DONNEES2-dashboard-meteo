import os
import requests
import pandas as pd
from datetime import datetime
import logging

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def extract_weather_and_air(city: str, api_key: str, date: str) -> bool:
    """
    Extrait les données météo et qualité de l'air pour une ville

    Args:
        city (str): Nom de la ville
        api_key (str): Clé API OpenWeather
        date (str): Date au format 'YYYY-MM-DD'

    Returns:
        bool: True si succès, False sinon
    """
    try:
        # 1. Récupération des coordonnées géographiques de la ville
        geo_url = "http://api.openweathermap.org/geo/1.0/direct"
        geo_params = {"q": city, "limit": 1, "appid": api_key}
        geo_response = requests.get(geo_url, params=geo_params, timeout=10)
        geo_response.raise_for_status()
        geo_data = geo_response.json()[0]
        lat, lon = geo_data["lat"], geo_data["lon"]

        # 2. Extraction données météo
        weather_url = "http://api.openweathermap.org/data/2.5/weather"
        weather_params = {
            "lat": lat,
            "lon": lon,
            "appid": api_key,
            "units": "metric",
            "lang": "fr",
        }
        weather_response = requests.get(weather_url, params=weather_params, timeout=10)
        weather_response.raise_for_status()
        weather_data = weather_response.json()

        # 3. Extraction qualité de l'air
        air_url = "http://api.openweathermap.org/data/2.5/air_pollution"
        air_params = {"lat": lat, "lon": lon, "appid": api_key}
        air_response = requests.get(air_url, params=air_params, timeout=10)
        air_response.raise_for_status()
        air_data = air_response.json()

        # 4. Structuration des données
        combined_data = {
            "ville": city,
            "date_extraction": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "date_observation": datetime.utcfromtimestamp(weather_data["dt"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "temperature": weather_data["main"]["temp"],
            "humidite": weather_data["main"]["humidity"],
            "pression": weather_data["main"]["pressure"],
            "vent_vitesse": weather_data["wind"]["speed"],
            "vent_direction": weather_data["wind"].get("deg", None),
            "conditions": weather_data["weather"][0]["description"],
            "aqi": air_data["list"][0]["main"]["aqi"],
            "co": air_data["list"][0]["components"]["co"],
            "no2": air_data["list"][0]["components"]["no2"],
            "o3": air_data["list"][0]["components"]["o3"],
            "pm2_5": air_data["list"][0]["components"]["pm2_5"],
            "pm10": air_data["list"][0]["components"]["pm10"],
            "so2": air_data["list"][0]["components"]["so2"],
        }

        # 5. Sauvegarde
        output_dir = f"{BASE_PATH}/raw/{date}"
        os.makedirs(output_dir, exist_ok=True)
        pd.DataFrame([combined_data]).to_csv(
            f"{output_dir}/weather_air_{city}.csv", index=False
        )
        return True
    except Exception as e:
        logging.error(f"Erreur: {str(e)}")
        return False
