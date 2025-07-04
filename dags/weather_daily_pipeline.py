from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from scripts.extract import extract_weather_data
from scripts.clean import clean_data
from scripts.merge import merge_data

BASE_PATH = "/home/mihajatiana/airflow/DONNEES2-dashboard-meteo/dags/data"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
    "max_active_tis_per_dag": 1,
}

with DAG(
    "weather_daily_pipeline",
    default_args=default_args,
    description="Pipeline quotidien pour données météo récentes",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "daily"],
) as dag:

    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]
    config = {
        "api_key": Variable.get("OPENWEATHER_API_KEY"),
        "date": "{{ ds }}",
    }

    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{city.lower()}",
            python_callable=extract_weather_data,
            op_kwargs={
                "city": city,
                "api_key": config["api_key"],
                "date": config["date"],
            },
            execution_timeout=timedelta(minutes=10),
            retries=3,
        )
        for city in cities
    ]

    clean_task = PythonOperator(
        task_id="clean_daily_data",
        python_callable=clean_data,
        op_kwargs={"date": config["date"], "is_historical": False},
        execution_timeout=timedelta(minutes=15),
    )

    merge_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={"date": config["date"]},
        execution_timeout=timedelta(minutes=20),
    )

    extract_tasks >> clean_task >> merge_task

    dag.doc_md = """
    ## Pipeline Météo Quotidien
    Traitement journalier des données météo:
    1. Extraction pour chaque ville
    2. Nettoyage des données
    3. Fusion avec l'historique
    
    Nécessite que l'historique ait été initialisé au préalable.
    """
