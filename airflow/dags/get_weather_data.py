from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from meteostat import Stations, Hourly

from utils import upload_to_gcs_op

def get_weather_data_tg(year=None, month=None):
    group_id = 'get_weather_data' if year is None else f'get_weather_data_{year}_{month}'
    
    with TaskGroup(group_id) as get_weather_data_tg:
        task_id_suffix = '' if year is None else f'_{year}_{month}'
        
        task_get_weather_data = PythonOperator(
            task_id=f'fetch_weather_data{task_id_suffix}',
            python_callable=fetch_weather_data,
            op_kwargs={
                'year': year if year is not None else "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='year') }}",
                'month': month if month is not None else "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='month') }}"
            },
            # pool='meteostat_fetch_pool',
        )
        
        local_path = f'/opt/airflow/data/raw/weather_data_{year}_{month}.csv' if year is not None else '/opt/airflow/data/raw/weather_data_{{ ti.xcom_pull(task_ids="get_latest_year_month", key="year") }}_{{ ti.xcom_pull(task_ids="get_latest_year_month", key="month") }}.csv'
        gcs_path = f'raw/weather_data_{year}_{month}.csv' if year is not None else 'raw/weather_data_{{ ti.xcom_pull(task_ids="get_latest_year_month", key="year") }}_{{ ti.xcom_pull(task_ids="get_latest_year_month", key="month") }}.csv'
        
        task_upload_weather_data_to_gcs = upload_to_gcs_op(
            f'weather_data{task_id_suffix}',
            "flight-delay-pred-data", 
            local_path, 
            gcs_path
        )
        
        task_get_weather_data >> task_upload_weather_data_to_gcs
        
    return get_weather_data_tg

def fetch_weather_data(year, month):
    df = pd.read_csv('/opt/airflow/data/raw/airport_coords.csv')
    stations = Stations()

    weather_lst = []
    start_date = pd.to_datetime(f'{year}-{month}-01') - pd.Timedelta(hours=3)
    end_date = pd.to_datetime(f'{year}-{month}-01') + pd.DateOffset(months=1) - pd.Timedelta(seconds=1) + pd.Timedelta(hours=3)

    print(f"Fetching weather data for {start_date} to {end_date}")
    for _, row in df.iterrows():
        for station in stations.nearby(row['Latitude'], row['Longitude']).fetch(5).index:
            data = Hourly(station, start_date, end_date).fetch()
            if data is not None:
                data.reset_index(inplace=True)
                data['IATA_code'] = row['IATA_code']
                weather_lst.append(data)
                break
            else:
                print(f"No weather data found for airport {row['IATA_code']} at station {station}")

    weather_df = pd.concat(weather_lst, ignore_index=True)
    if 'date' in weather_df.columns and 'hour' in weather_df.columns:
        weather_df.drop(columns=['date', 'hour'], inplace=True)
    weather_df.to_csv(f'/opt/airflow/data/raw/weather_data_{year}_{month}.csv', index=False)

