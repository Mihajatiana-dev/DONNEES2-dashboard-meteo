import pandas as pd
import os
from datetime import datetime
import logging

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def generate_recommendations():
    """
    Version corrigée avec :
    - Filtre moins restrictif
    - Meilleure gestion des données manquantes
    - Logs de débogage
    """
    try:
        # 1. Chargement des données avec vérification
        input_file = f"{BASE_PATH}/final/historical_weather.csv"
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Fichier historique introuvable : {input_file}")

        df = pd.read_csv(
            input_file,
            parse_dates=["date_observation"],
            dtype={"ville": "category", "saison": "category"},
        )

        # Vérification des données
        if df.empty:
            logging.warning("Aucune donnée dans historical_weather.csv")
            return None

        # 2. Calcul des indicateurs de base
        df["est_weekend"] = df["date_observation"].dt.dayofweek >= 5
        df["jours_ouvres"] = (~df["est_weekend"]).astype(int)

        # 3. Agrégation mensuelle
        recommendations = (
            df.groupby(["ville", "mois", "saison"], observed=True)
            .agg(
                temperature_moyenne=("temperature_moy", "mean"),
                temp_min=("temperature_moy", "min"),
                temp_max=("temperature_moy", "max"),
                precipitations_totales=("precipitation", "sum"),
                vent_moyen=("vent_moyen", "mean"),
                score_meteo_moyen=("score_meteo", "mean"),
                jours_recommandes=("periode_recommandee", "sum"),
                jours_ouvres_totaux=("jours_ouvres", "sum"),
            )
            .reset_index()
        )

        # 4. Calcul des pourcentages (avec protection division par zéro)
        recommendations["pourcentage_recommandé"] = (
            (
                recommendations["jours_recommandes"]
                / recommendations["jours_ouvres_totaux"].replace(0, 1)
            )
            * 100
        ).round(1)

        # 5. Classement et sauvegarde (sans filtre strict)
        recommendations["classement"] = (
            recommendations.groupby(["saison", "mois"])["score_meteo_moyen"]
            .rank(ascending=False, method="min")
            .astype(int)
        )

        # 6. Sauvegarde avec vérification
        output_file = f"{BASE_PATH}/final/tourism_recommendations.csv"

        # Garder toutes les données même si jours_recommandes < 5
        recommendations.to_csv(output_file, index=False)

        # Logs de vérification
        logging.info(f"Recommandations sauvegardées dans {output_file}")
        logging.info(f"Nombre de lignes : {len(recommendations)}")
        logging.info(f"Exemple de données :\n{recommendations.head(3)}")

        return output_file

    except Exception as e:
        logging.error(f"Erreur lors de la génération des recommandations : {str(e)}")
        raise
