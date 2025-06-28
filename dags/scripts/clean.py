import pandas as pd
import os

def clean_data(date: str) -> str:
    """
    Nettoie les données brutes sans calculer d'indicateurs avancés
    
    Args:
        date (str): Date au format 'YYYY-MM-DD'
        
    Returns:
        str: Chemin du fichier nettoyé
    """
    input_dir = f"dags/data/raw/{date}"
    output_dir = "dags/data/processed"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Chargement et concaténation
    all_data = []
    for file in os.listdir(input_dir):
        if file.startswith('weather_air_') and file.endswith('.csv'):
            df = pd.read_csv(f"{input_dir}/{file}")
            all_data.append(df)
    
    if not all_data:
        raise ValueError(f"Aucune donnée à nettoyer pour {date}")
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 2. Nettoyage de base
    # Conversion des dates
    combined_df['date_extraction'] = pd.to_datetime(combined_df['date_extraction'])
    combined_df['date_observation'] = pd.to_datetime(combined_df['date_observation'])
    
    # 3. Ajout de colonnes utiles pour les futurs calculs (sans calculer les indicateurs)
    combined_df['annee'] = combined_df['date_observation'].dt.year
    combined_df['mois'] = combined_df['date_observation'].dt.month
    combined_df['jour'] = combined_df['date_observation'].dt.day
    
    # 4. Sauvegarde
    output_path = f"{output_dir}/cleaned_weather_air_{date}.csv"
    combined_df.to_csv(output_path, index=False)
    
    return output_path