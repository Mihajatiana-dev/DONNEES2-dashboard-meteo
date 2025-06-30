from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from scripts.extract import extract_weather_data
from scripts.clean import clean_data
from scripts.merge import merge_data
from scripts.calculate_tourism import generate_recommendations

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,  # Activé pour suivre les erreurs
    "email_on_retry": True,
    "max_active_tis_per_dag": 1,  # Prévention des conflits
}

with DAG(
    "tourism_weather_pipeline",
    default_args=default_args,
    description="Pipeline pour recommandations touristiques basées sur la météo",
    schedule="@daily",  # Plus explicite que schedule
    catchup=False,
    max_active_runs=1,
    tags=["tourism", "weather", "recommendation"],
) as dag:

    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]

    # Configuration dynamique
    config = {
        "api_key": Variable.get("OPENWEATHER_API_KEY"),
        "date": "{{ ds }}",
        "email_alert": Variable.get("ALERT_EMAIL", default_var="admin@example.com"),
    }

    # Tâches
    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{city.lower()}",
            python_callable=extract_weather_data,
            op_kwargs={
                "city": city,
                "api_key": config["api_key"],
                "date": config["date"],
            },
            execution_timeout=timedelta(minutes=10),  # Timeout explicite
            retries=3,
        )
        for city in cities
    ]

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"date": config["date"]},
        execution_timeout=timedelta(minutes=15),
    )

    merge_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={"date": config["date"]},
        execution_timeout=timedelta(minutes=20),
    )

    tourism_task = PythonOperator(
        task_id="generate_recommendations",
        python_callable=generate_recommendations,
        retries=1,
    )

    # Workflow optimisé
    extract_tasks >> clean_task >> merge_task >> tourism_task

    # Documentation intégrée
    dag.doc_md = """
    ## Pipeline Tourisme Météo
    Génère des recommandations touristiques basées sur :
    - Température idéale (22-28°C)
    - Faible précipitation (<5mm/jour)
    - Vent modéré (<15 km/h)
    """
