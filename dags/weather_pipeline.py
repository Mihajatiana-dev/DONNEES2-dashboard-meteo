from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from scripts.extract import extract_weather_and_air
from scripts.clean import clean_data
from scripts.merge import merge_data
from scripts.validate import validate_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "weather_air_quality_pipeline",
    default_args=default_args,
    description="Pipeline de collecte et traitement des données météo et qualité de l'air",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "air_quality", "etl"],
) as dag:

    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]

    extract_tasks = []
    for city in cities:
        task = PythonOperator(
            task_id=f"extract_{city.lower()}",
            python_callable=extract_weather_and_air,
            op_kwargs={
                "city": city,
                "api_key": Variable.get("OPENWEATHER_API_KEY"),
                "date": "{{ ds }}",  # Date d'exécution du DAG
            },
        )
        extract_tasks.append(task)

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"date": "{{ ds }}"},
    )

    merge_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={"date": "{{ ds }}"},
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        op_kwargs={"file_path": "dags/data/final/historical_weather_air.csv"},
    )

    # Définition des dépendances
    extract_tasks >> clean_task >> merge_task >> validate_task
