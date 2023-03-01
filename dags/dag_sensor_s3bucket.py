from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from datetime import datetime, date
from scripts_dag_transform_data import transform_data_to_silver, local_to_s3, copy_to_local_from_s3bucket, transform_csv_to_gold, populate_table
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Variables Declaration
path = Variable.get('path_randomuser')
bucket_name = Variable.get('bucket_name_randomuser')
file_path = f'{path}*.*'
key_bronze = f'randomuser/bronze/' + str(date.today()) + '_randomuser.parquet'
key_silver = f'randomuser/silver/table_silver.csv'
key_gold = f'randomuser/gold/table_gold.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'description': 'DAG to copy files from S3 to local filesystem'
}

with DAG('transform_data_to_gold_layer', default_args=default_args, schedule_interval=None, template_searchpath='/opt/airflow/sql') as dag:

    # Task responsible for waiting for a new file in the bucket.
    detect_new_file = S3KeySensor(
        task_id='detect_new_file',
        bucket_name='thiago-ferreira-estudos',
        bucket_key=key_bronze,
        poke_interval=10
    )

    # Task responsible for accessing the bronze layer of the bucket and copying it to the local directory.
    copy_to_local_task_from_bronze = PythonOperator(
        task_id='copy_to_local_task_from_bronze',
        python_callable=copy_to_local_from_s3bucket,
        op_kwargs={'bucket_name': bucket_name, 'key': key_bronze, 'local_path': path}
    )

    # Task responsible for accessing the silver layer of the bucket and copying it to the local directory.
    copy_to_local_task_from_silver = PythonOperator(
        task_id='copy_to_local_task_from_silver',
        python_callable=copy_to_local_from_s3bucket,
        op_kwargs={'bucket_name': bucket_name, 'key': key_silver, 'local_path': path}
    )

    # Task responsible for doing data transformation. 
    task_transform_parquet_csv = PythonOperator(
        task_id='task_transform_parquet_csv',
        python_callable=transform_data_to_silver,
        op_kwargs={'filepath': file_path}
    )

    # Task responsible for sending the processed file from the previous task to the silver layer in the bucket.
    task_upload_silver = PythonOperator(
        task_id='task_upload_silver',
        python_callable=local_to_s3,
        op_kwargs={'filename': './data/randomuser/table_silver.csv', 'bucket_name': bucket_name, 'key': 'randomuser/silver/table_silver.csv', 'filepath': file_path}
    )

    # Task responsible for accessing the silver layer of the bucket and copying it to the local directory.
    copy_to_local_task_from_silver2 = PythonOperator(
        task_id='copy_to_local_task_from_silver2',
        python_callable=copy_to_local_from_s3bucket,
        op_kwargs={'bucket_name': bucket_name, 'key': key_silver, 'local_path': path}
    )

    # Task responsible for accessing the gold layer of the bucket and copying it to the local directory.
    copy_to_local_task_from_gold = PythonOperator(
        task_id='copy_to_local_task_from_gold',
        python_callable=copy_to_local_from_s3bucket,
        op_kwargs={'bucket_name': bucket_name, 'key': key_gold, 'local_path': path}
    )

    # Task responsible for performing data transformation for the gold table.
    task_transform_csv_gold = PythonOperator(
        task_id='task_transform_csv_gold',
        python_callable=transform_csv_to_gold
    )

    # Task responsible for sending the processed file from the previous task to the gold layer in the bucket.
    task_upload_gold = PythonOperator(
        task_id='task_upload_gold',
        python_callable=local_to_s3,
        op_kwargs={'filename': './data/randomuser/table_gold_BR.csv', 'bucket_name': bucket_name, 'key': 'randomuser/gold/table_gold_BR.csv', 'filepath': file_path, 'flag': False}
    )

    # Task responsible for creating a table (if it doesn't exist) in the PostgreSQL database, calling the available query in the SQL directory.
    create_table_dw_users = PostgresOperator(
        task_id = 'create_table_dw_users',
        postgres_conn_id = 'postgres-airflow',
        sql = 'criar_tabela_dw_users.sql'
    )

    # Task responsible for populating the table in the database.
    task_populate_table=PythonOperator(
        task_id='task_populate_table',
        python_callable=populate_table
        )

# Set up the task dependencies.
detect_new_file >> [copy_to_local_task_from_bronze, copy_to_local_task_from_silver] >> task_transform_parquet_csv >> \
    task_upload_silver >> [copy_to_local_task_from_silver2, copy_to_local_task_from_gold] >> task_transform_csv_gold >> \
        task_upload_gold >> create_table_dw_users >> task_populate_table