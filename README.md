# **WEATHER DASHBOARD**

## Climate and Tourism — When to Travel?

#### Project structure :

---- /dags
| |--- /weather*daily_pipeline.py
| |--- /weather_historical_init.py
| |--- /scripts
| | |--- /extract.py
| | |--- /clean.py
| | |--- /merge.py
| | |--- /transform.py
| |--- /data
| | |--- /raw
| | | |--- /{date}
| | | | |--- /weather*{city}.csv
| | |--- /processed
| | | |--- /cleaned*weather*{date}.csv
| | |--- /historical
| | | |--- /historical_date
| | | | |--- /{city} 2020-01-01 to 2025-06-29.csv
| | | |--- /historical.csv
| | | |--- /historical_raw.csv
| | | |--- /historical_cleaned.csv
| | | |--- /merge_historical.py
| | |--- /final
| | | |--- /historical_weather.csv
| | | |--- /star_schema
| | | | |--- /\_metadata.json
| | | | |--- /fact_weather.csv
| | | | |--- /dim_city.csv
| | | | |--- /dim_date.csv
| | | | |--- /dim_conditions.csv
|--- /EDA
| |--- /init_EDA.ipynb
| |--- /final_EDA.ipynb
|--- /images
| |--- /weather_diagram.png
---- /README.md

### DAG Airflow :

### EDA

### DIAGRAM

### DASHBOARD

DAG Airflow:
Mode de fonctionnement :

- S’assurer d’avoir toutes les dépendances nécessaires

- Lancer le DAG avec:

* api-server
* scheduler
* dag-processor

- activer weather_historical_init, qui permettra d’extraire toutes les historiques de 2020-01-01 à 2025-06-29(origine: visualcrossing.com), puis le nettoyer, prêt à être fusionné avec les donnees récentes
- activer weather_daily_init qui va non seulement fusionner toutes les donnees récentes du dossier processed avec les donnees historiques nettoyées de historical_cleaned, mais récupérera aussi les donnees du jour et former historical_weather.csv pour le tout premier run; les prochains fusionneront que les donnees du jour avec historical_weather.csv
- Le modèle étoile sera aussi créé dans /final/star_schema, à partir de historical_weather.csv
- En cas de modification des donnees de historical.csv(données historiques), des donnees dans raw ou processed(donnees récentes), il est conseillé de supprimer historical_weather.csv mais aussi les fichiers de /star_schema et reprendre le workflow en relançant weather_historical_init puis weather_daily_pipeline pour éviter de corrompre les donnees

Diagramme :
Explications: alors on a une table des faits, qui est la météo et 3 dimensions qui sont : la ville, la date et la condition
C’est un modèle en étoile car…

EDA:
Alors j’ai utilisé deux EDA différents:

- initial_EDA.ipynb: qui m’a permis d’analyser les donnees historiques avant d’avancer dans le projet
  les types d’analyse effectué sont:

* nombre de lignes et de colonnes
* Les différentes colonnes
* Vérification de potentielles anomalie(température…)
*

- final_EDA.ipynb: qui m’a permis d’illustrer les donnees finales à travers des graphiques comme:

*

Dadhboard :
