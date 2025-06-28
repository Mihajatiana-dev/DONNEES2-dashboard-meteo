import pandas as pd


def validate_data(file_path: str) -> bool:
    """
    Validation basique des données finales
    """
    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("Le fichier final est vide")

    required_columns = {"ville", "date_observation", "temperature", "humidite", "aqi"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Colonnes manquantes: {missing}")

    return True
