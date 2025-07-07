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
