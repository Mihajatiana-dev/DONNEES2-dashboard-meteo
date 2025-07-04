from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import extract_historical_data
from scripts.clean import clean_data

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    "weather_historical_initialization",
    default_args=default_args,
    description="Initialisation unique des données historiques",
    schedule=None,  # Exécution manuelle uniquement
    catchup=False,
    tags=["weather", "historical", "init"],
) as dag:

    extract_historical = PythonOperator(
        task_id="extract_historical_data",
        python_callable=extract_historical_data,
        op_kwargs={"output_path": f"{BASE_PATH}/historical/historical_raw.csv"},
        execution_timeout=timedelta(minutes=30),
    )

    clean_historical = PythonOperator(
        task_id="clean_historical_data",
        python_callable=clean_data,
        op_kwargs={"date": "historical", "is_historical": True},
        execution_timeout=timedelta(minutes=30),
    )

    extract_historical >> clean_historical

    dag.doc_md = """
    ## Initialisation des données historiques
    À exécuter MANUELLEMENT une seule fois pour:
    1. Extraire les données historiques
    2. Les nettoyer et les préparer
    
    Ne pas planifier ce DAG - traitement unique.
    """
