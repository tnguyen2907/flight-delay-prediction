from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator

from utils import download_unzip_tg, upload_to_gcs_op

def get_flight_data_tg(year=None, month=None):
    group_id = 'get_flight_data' if year is None else f'get_flight_data_{year}_{month}'
    with TaskGroup(group_id) as get_flight_data_tg:
        suffix = "flight_data" if year is None else f'flight_data_{year}_{month}'
        
        if year is not None:
            file_name = f'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip'
            file_path = f'/opt/airflow/data/raw/{file_name}'
            download_url = f'https://transtats.bts.gov/PREZIP/{file_name}'
            csv_source = f'/opt/airflow/data/raw/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year}_{month}.csv'
            csv_dest = f'/opt/airflow/data/raw/flight_data_{year}_{month}.csv'
            gcs_path = f'raw/flight_data_{year}_{month}.csv'
        else:
            year_xcom = "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='year') }}"
            month_xcom = "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='month') }}"
            file_name = f'On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year_xcom}_{month_xcom}.zip'
            file_path = f'/opt/airflow/data/raw/{file_name}'
            download_url = f'https://transtats.bts.gov/PREZIP/{file_name}'
            csv_source = f'/opt/airflow/data/raw/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{year_xcom}_{month_xcom}.csv'
            csv_dest = f'/opt/airflow/data/raw/flight_data_{year_xcom}_{month_xcom}.csv'
            gcs_path = f'raw/flight_data_{year_xcom}_{month_xcom}.csv'

        task_group_download = download_unzip_tg(suffix, file_path, download_url)

        task_rename = BashOperator(
            task_id=f'rename_{suffix}',
            bash_command=f'mv "{csv_source}" "{csv_dest}"',
        )

        task_upload_to_gcs = upload_to_gcs_op(
            suffix, 
            "flight-delay-pred-data", 
            csv_dest, 
            gcs_path
        )

        task_group_download >> task_rename >> task_upload_to_gcs

    return get_flight_data_tg