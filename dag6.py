from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from airflow.hooks.base_hook import BaseHook
import yaml
import os
import re

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
patrick2_aws_access_key_id = global_var_data['patrick2_aws_access_key_id']
patrick2_aws_secret_access_key = global_var_data['patrick2_aws_secret_access_key']
patrick2_aws_session_token = global_var_data['patrick2_aws_session_token']


def access_s3():
    aws_conn_id = 'AWS_Patrick_1'
    aws_connection = BaseHook.get_connection(aws_conn_id)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=patrick2_aws_access_key_id,
        aws_secret_access_key=patrick2_aws_secret_access_key,
        aws_session_token=patrick2_aws_session_token,
        region_name=aws_connection.extra_dejson.get('region_name', 'us-west-2')
    )

    bucket_name = 'velospotbucket'
    objects_in_bucket = s3_client.list_objects(Bucket=bucket_name)
    download_directory = '/home/ubuntu/airflow/velospot/'

    # Paginierung verwenden, um mehr als 1000 Objekte abzurufen
    paginator = s3_client.get_paginator('list_objects')
    pages = paginator.paginate(Bucket=bucket_name)

    total_files = 0  # Zähler für Gesamtzahl der Dateien

    for page_num, page in enumerate(pages, start=1):
        print(f"Seite {page_num}: {page}")
    
        for obj in page.get('Contents', []):
            file_name = obj['Key']
        
            # Den Dateinamen bereinigen, um ungültige Zeichen zu entfernen
            clean_file_name = re.sub(r'[<>:"/\\|?*]', '', file_name)
        
            # Pfad zum Speichern der Datei mit bereinigtem Dateinamen erstellen
            file_path = os.path.join(download_directory, clean_file_name)
        
            # Datei herunterladen
            s3_client.download_file(bucket_name, file_name, file_path)
        
            total_files += 1  # Zähler erhöhen
            print(f"Datei {total_files} heruntergeladen: {file_path}")

    print(f"Gesamtanzahl der heruntergeladenen Dateien: {total_files}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
    'retries': 1
}

with DAG('s3_velospot_download', default_args=default_args, schedule_interval=None) as dag:
    access_s3_task = PythonOperator(
        task_id='access_s3_task',
        python_callable=access_s3
    )

if __name__ == "__main__":
    dag.cli()

