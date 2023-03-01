from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from scripts_dag_ingestion_api import local_to_s3, remove_local_files, processing_user
from datetime import datetime
import json

# Variables Declaration
path = Variable.get('path_randomuser')
compression = Variable.get('compression_randomuser')
filename = Variable.get('filename_randomuser')
bucket_name = Variable.get('bucket_name_randomuser')
file_path = f'{path}*.*'

default_args = {
    'owner': 'Thiago-Ferreira',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime.now(),
    'catchup': False,
    'description': 'DAG responsible for get data api, processing data and load in s3 bucket.'
}

with DAG('ingest_data_api_to_bronze_layer', schedule_interval='@daily', default_args=default_args) as dag:
    # Task responsible for confirming the availability of the API.
    api_available = HttpSensor(
        task_id='api_available',
        http_conn_id='randomuser_api',
        endpoint='/api'        
    )

    # Task responsible for making the request for the API and retrieving the information in JSON format.
    get_users=SimpleHttpOperator(
        task_id='get_users',
        http_conn_id='randomuser_api',
        endpoint='/api/?results=5000',
        method='GET',
        response_filter=lambda response: json.loads(response.text)
    )

    # Task responsible for processing the data acquired by the previous task. 
    # It processes the data by converting the JSON to a DataFrame and subsequently it in a local folder in parquet format.
    processing_user_task = PythonOperator(
        task_id='processing_user_task',
        python_callable=processing_user,
        op_kwargs={'compression':compression, 'path':path, 'filename':filename}        
    )

    # Task responsible for connecting to the S3 bucket and sending the parquet file.
    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=local_to_s3,
        op_kwargs={'bucket_name': bucket_name, 'dir_target': 'randomuser/', 'layer': 'bronze', 'filepath': file_path}
    )

    # Task responsible for removing the local file parquet.
    remove_df_local=PythonOperator(
        task_id='remove_df_local',
        python_callable=remove_local_files,
        op_kwargs={'filepath': file_path}
    )

api_available >> get_users >> processing_user_task >> upload_to_s3 >> remove_df_local