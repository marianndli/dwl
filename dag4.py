from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from airflow.hooks.base_hook import BaseHook
import yaml
import os

# Funktion zum Lesen der globalen Variablen aus einer YAML-Datei
def read_global_var_from_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        return data

# Definiere den Pfad zur global_var.yaml-Datei im Ordner "airflow"
file_path = os.path.join('airflow', 'global_var.yaml')

# Lese die globalen Variablen aus der YAML-Datei
global_var_data = read_global_var_from_yaml(file_path)

# Greife auf die globalen Variablen zu und verwende sie
patrick1_aws_access_key_id = global_var_data['patrick1_aws_access_key_id']
patrick1_aws_secret_access_key = global_var_data['patrick1_aws_secret_access_key']
patrick1_aws_session_token = global_var_data['patrick1_aws_session_token']


def access_s3():
    aws_conn_id = 'AWS_Patrick_1'
    aws_connection = BaseHook.get_connection(aws_conn_id)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=patrick1_aws_access_key_id,
        aws_secret_access_key=patrick1_aws_secret_access_key,
	aws_session_token=patrick1_aws_session_token,
        region_name=aws_connection.extra_dejson.get('region_name', 'us-west-2')
    )

    bucket_name = 'datalakepartition'
    objects_in_bucket = s3_client.list_objects(Bucket=bucket_name)
    download_directory = '/home/ubuntu/airflow/interruptions/'


    for obj in objects_in_bucket['Contents']:
        print(obj['Key'])

    for obj in objects_in_bucket.get('Contents', []):
        file_name = obj['Key']
        file_path = os.path.join(download_directory, file_name)
        s3_client.download_file(bucket_name, file_name, file_path)
        print(f"Datei heruntergeladen: {file_path}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
    'retries': 1
}

with DAG('s3_interruptions_download', default_args=default_args, schedule_interval=None) as dag:
    access_s3_task = PythonOperator(
        task_id='access_s3_task',
        python_callable=access_s3
    )

if __name__ == "__main__":
    dag.cli()


