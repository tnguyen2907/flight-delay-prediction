from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from utils import upload_to_gcs_op

def save_weather_code_meteostat(file_path):
    weather_code_meteostat = weather_classification = [
    {"Code": 1, "WeatherCondition": "Clear",               "ExtremeWeather": False},
    {"Code": 2, "WeatherCondition": "Fair",                "ExtremeWeather": False},
    {"Code": 3, "WeatherCondition": "Cloudy",              "ExtremeWeather": False},
    {"Code": 4, "WeatherCondition": "Overcast",            "ExtremeWeather": False},
    {"Code": 5, "WeatherCondition": "Fog",                 "ExtremeWeather": True},
    {"Code": 6, "WeatherCondition": "Freezing Fog",        "ExtremeWeather": True},
    {"Code": 7, "WeatherCondition": "Light Rain",          "ExtremeWeather": False},
    {"Code": 8, "WeatherCondition": "Rain",                "ExtremeWeather": False},
    {"Code": 9, "WeatherCondition": "Heavy Rain",          "ExtremeWeather": True},
    {"Code": 10, "WeatherCondition": "Freezing Rain",      "ExtremeWeather": True},
    {"Code": 11, "WeatherCondition": "Heavy Freezing Rain","ExtremeWeather": True},
    {"Code": 12, "WeatherCondition": "Sleet",              "ExtremeWeather": True},
    {"Code": 13, "WeatherCondition": "Heavy Sleet",        "ExtremeWeather": True},
    {"Code": 14, "WeatherCondition": "Light Snowfall",     "ExtremeWeather": False},
    {"Code": 15, "WeatherCondition": "Snowfall",           "ExtremeWeather": True},
    {"Code": 16, "WeatherCondition": "Heavy Snowfall",     "ExtremeWeather": True},
    {"Code": 17, "WeatherCondition": "Rain Shower",        "ExtremeWeather": False},
    {"Code": 18, "WeatherCondition": "Heavy Rain Shower",  "ExtremeWeather": True},
    {"Code": 19, "WeatherCondition": "Sleet Shower",       "ExtremeWeather": True},
    {"Code": 20, "WeatherCondition": "Heavy Sleet Shower", "ExtremeWeather": True},
    {"Code": 21, "WeatherCondition": "Snow Shower",        "ExtremeWeather": False},
    {"Code": 22, "WeatherCondition": "Heavy Snow Shower",  "ExtremeWeather": True},
    {"Code": 23, "WeatherCondition": "Lightning",          "ExtremeWeather": True},
    {"Code": 24, "WeatherCondition": "Hail",               "ExtremeWeather": True},
    {"Code": 25, "WeatherCondition": "Thunderstorm",       "ExtremeWeather": True},
    {"Code": 26, "WeatherCondition": "Heavy Thunderstorm", "ExtremeWeather": True},
    {"Code": 27, "WeatherCondition": "Storm",              "ExtremeWeather": True}
]


    df = pd.DataFrame(weather_code_meteostat)
    df.to_csv(file_path, index=False)

def get_weather_code_meteostat_tg():
    file_path="/opt/airflow/data/raw/weather_code_meteostat.csv"
    with TaskGroup('get_weather_code_meteostat') as get_weather_code_meteostat_tg:
        task_save_weather_code_meteostat = PythonOperator(
            task_id='save_weather_code_meteostat',
            python_callable=save_weather_code_meteostat,
            op_kwargs={'file_path': file_path},
        )

        task_upload_to_gcs_weather_code_meteostat = upload_to_gcs_op('weather_code_meteostat', "flight-delay-pred-data", file_path, 'raw/weather_code_meteostat.csv')

        task_save_weather_code_meteostat >> task_upload_to_gcs_weather_code_meteostat

    return get_weather_code_meteostat_tg
