from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

import os
import zipfile
from datetime import datetime

def unzip_data(file_path, output_dir='/opt/airflow/data/raw/'):
    print(file_path)
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

def download_op(suffix, file_path, download_url, user_agent=False):
    USER_AGENT = "'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'"
    return BashOperator(
        task_id='download_' + suffix,
        bash_command=f'curl {"-A " + USER_AGENT if user_agent else ""} -k -o {file_path} {download_url}',
    )

def download_unzip_tg(suffix, file_path, download_url, user_agent=False):
    with TaskGroup(f"download_unzip_{suffix}", tooltip="Download, unzip, then remove zip file") as download_unzip_tg:
        task_download_data = download_op(suffix, file_path, download_url, user_agent)        

        task_unzip_data = PythonOperator(
            task_id=f'unzip_{suffix}',
            python_callable=unzip_data,
            op_kwargs={'file_path': file_path},
        )

        task_rm_zip = BashOperator(
            task_id=f'rm_zip_{suffix}',
            bash_command=f'rm {file_path}',
        )

        task_download_data >> task_unzip_data >> task_rm_zip

    return download_unzip_tg

def upload_to_gcs(bucket_name, local_path, gcs_name):
    GCSHook(gcp_conn_id='google_cloud').upload(bucket_name, gcs_name, local_path, timeout=1800)

def upload_to_gcs_op(suffix, bucket_name, local_path, gcs_name):
    return PythonOperator(
        task_id='upload_to_gcs_' + suffix,
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket_name': bucket_name,
            'local_path': local_path,
            'gcs_name': gcs_name,
        },
        # pool='gcs_upload_pool',
    )

def submit_spark_job_op(suffix, spark_job_path, params={}):
    if params:
        
        application_args = []
        for key, value in params.items():
            application_args.extend([f"--{key}", value])
    else:
        
        application_args = [
            "--year", "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='year') }}",
            "--month", "{{ ti.xcom_pull(task_ids='get_latest_year_month', key='month') }}"
        ]

    return SparkSubmitOperator(
        task_id='spark_' + suffix,
        application=spark_job_path,
        conn_id='spark',
        verbose=True,
        conf={
            "spark.jars": "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.executor.memory": "9g",
        },
        application_args=application_args,
    )

def submit_dataproc_job_op(suffix, spark_job_path, params={}):
    if params:
        args = [f"--{key}={value}" for key, value in params.items()]
    else:
        args = [
            "--year={{ ti.xcom_pull(task_ids='get_latest_year_month', key='year') }}",
            "--month={{ ti.xcom_pull(task_ids='get_latest_year_month', key='month') }}"
        ]
    
    BATCH_CONFIG = {
        "pyspark_batch": {
            "main_python_file_uri": spark_job_path,
            "args": args,
        },
        "environment_config": {
            "execution_config": {
                "service_account": f"dataproc-sa@{os.environ['GCP_PROJECT_ID']}.iam.gserviceaccount.com",
            }
        },
        "runtime_config": {
            "properties": {
                "spark.executor.instances": "2", 
                "spark.executor.cores": "4",     
                "spark.driver.cores": "4",
            }
        }
    }
    
    return DataprocCreateBatchOperator(
        task_id='dataproc_' + suffix,
        project_id=os.environ['GCP_PROJECT_ID'],
        region='us-east1',
        batch=BATCH_CONFIG,
        batch_id=f'dataproc-{suffix}-' + datetime.now().strftime('%Y%m%d%H%M%S'),
        gcp_conn_id='google_cloud',
    )
                
        
def load_sql(sql_file):
    with open(sql_file, 'r') as file:
        return file.read()