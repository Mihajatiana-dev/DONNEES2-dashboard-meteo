import pandas as pd
import os

def merge_data(date: str) -> str:
    """
    Fusionne les nouvelles données avec l'historique
    
    Args:
        date (str): Date au format 'YYYY-MM-DD'
        
    Returns:
        str: Chemin du fichier fusionné
    """
    # 1. Chargement des nouvelles données
    input_file = f"dags/data/processed/cleaned_weather_air_{date}.csv"
    new_data = pd.read_csv(input_file)
    
    # 2. Chargement de l'historique (si existe)
    historical_file = "dags/data/final/historical_weather_air.csv"
    if os.path.exists(historical_file):
        historical_data = pd.read_csv(historical_file)
        # Fusion en évitant les doublons
        merged_data = pd.concat([historical_data, new_data]).drop_duplicates(
            subset=['ville', 'date_observation'],
            keep='last'
        )
    else:
        merged_data = new_data
    
    # 3. Sauvegarde
    os.makedirs("dags/data/final", exist_ok=True)
    merged_data.to_csv(historical_file, index=False)
    
    return historical_file