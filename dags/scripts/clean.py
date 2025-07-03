import pandas as pd
import os

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"


def clean_data(date: str, is_historical: bool = False) -> str:
    """
    Nettoie et enrichit les données pour l'analyse touristique.
    Mode historique (is_historical=True) ou données récentes (is_historical=False).
    """
    if is_historical:
        # Mode historique - Traitement du fichier unique
        input_path = f"{BASE_PATH}/historical/historical_raw.csv"
        output_path = f"{BASE_PATH}/historical/historical_cleaned.csv"

        # Chargement
        try:
            combined_df = pd.read_csv(input_path)
            # Vérification des colonnes requises
            required_columns = {
                "ville",
                "date_observation",
                "temperature_moy",
                "precipitation",
                "vent_moyen",
            }
            if not required_columns.issubset(combined_df.columns):
                raise ValueError("Colonnes manquantes dans le fichier historique")

        except Exception as e:
            print(f"Erreur lecture historique : {str(e)}")
            raise
    else:
        # Mode données récentes - Traitement normal (inchangé)
        input_dir = f"{BASE_PATH}/raw/{date}"
        output_dir = f"{BASE_PATH}/processed"
        os.makedirs(output_dir, exist_ok=True)

        # 1. Chargement des fichiers par ville
        all_data = []
        for file in os.listdir(input_dir):
            if file.startswith("weather_") and file.endswith(".csv"):
                try:
                    df = pd.read_csv(f"{input_dir}/{file}")
                    required_columns = {
                        "ville",
                        "date_observation",
                        "temperature_moy",
                        "precipitation",
                        "vent_moyen",
                    }
                    if not required_columns.issubset(df.columns):
                        raise ValueError(f"Colonnes manquantes dans {file}")
                    all_data.append(df)
                except Exception as e:
                    print(f"Erreur lecture {file}: {str(e)}")
                    continue

        if not all_data:
            raise ValueError(f"Aucune donnée valide à nettoyer pour {date}")

        combined_df = pd.concat(all_data, ignore_index=True)
        output_path = f"{output_dir}/cleaned_weather_{date}.csv"

    # --- Partie commune de nettoyage ---
    # 2. Conversion des dates
    date_columns = ["date_extraction", "date_observation"]
    for col in date_columns:
        if col in combined_df.columns:
            combined_df[col] = pd.to_datetime(combined_df[col], errors="coerce")

    # 3. Nettoyage des données
    combined_df = combined_df.dropna(subset=["date_observation"])

    # 4. Calcul des indicateurs temporels
    combined_df["annee"] = combined_df["date_observation"].dt.year
    combined_df["mois"] = combined_df["date_observation"].dt.month
    combined_df["jour"] = combined_df["date_observation"].dt.day
    combined_df["saison"] = combined_df["mois"].apply(
        lambda m: (
            "Hiver"
            if m in [12, 1, 2]
            else (
                "Printemps"
                if m in [3, 4, 5]
                else "Été" if m in [6, 7, 8] else "Automne"
            )
        )
    )

    # 5. Calcul des indicateurs touristiques
    combined_df["temp_ideale"] = combined_df["temperature_moy"].between(22, 28)
    combined_df["peu_vent"] = combined_df["vent_moyen"] < 15
    combined_df["peu_pluie"] = combined_df["precipitation"] < 5  # Seuil à 5mm/jour
    combined_df["bonne_visibilite"] = combined_df["visibilite_moy"] > 5

    combined_df["score_meteo"] = (
        combined_df["temp_ideale"].astype(int) * 30
        + combined_df["peu_pluie"].astype(int) * 30
        + combined_df["peu_vent"].astype(int) * 20
        + combined_df["bonne_visibilite"].astype(int) * 20
    )

    combined_df["periode_recommandee"] = (
        combined_df["temp_ideale"]
        & combined_df["peu_pluie"]
        & combined_df["peu_vent"]
        & combined_df["bonne_visibilite"]
    )

    # 6. Sélection des colonnes finales
    output_columns = [
        "ville",
        "date_extraction",
        "date_observation",
        "temperature_moy",
        "temp_min",
        "temp_max",
        "humidite_moy",
        "precipitation",
        "vent_moyen",
        "visibilite_moy",
        "conditions",
        "annee",
        "mois",
        "jour",
        "saison",
        "temp_ideale",
        "peu_vent",
        "peu_pluie",
        "bonne_visibilite",
        "score_meteo",
        "periode_recommandee",
    ]
    combined_df = combined_df[output_columns]

    # 7. Sauvegarde
    combined_df.to_csv(output_path, index=False)
    print(f"Fichier nettoyé généré : {output_path}")
    print(
        f"Statistiques : {combined_df[['precipitation', 'periode_recommandee']].describe()}"
    )

    return output_path
