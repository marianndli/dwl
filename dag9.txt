from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from airflow.hooks.base_hook import BaseHook
import yaml
import os
import pandas as pd
import psycopg2
import csv
from sqlalchemy import create_engine


# Funktion zum Lesen der globalen Variablen aus einer YAML-Datei
def read_global_var_from_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        return data

# Definiere den Pfad zur global_var.yaml-Datei im Ordner "airflow"
file_path = os.path.join('airflow', 'global_var.yaml')

# Lese die globalen Variablen aus der YAML-Datei
global_var_data = read_global_var_from_yaml(file_path)

# Greife auf die globalen Variablen zu und verwende sie. Hier muss das Passwort der richtigen Person aufgeführt werden
connectstring = global_var_data['kira_rds1_password']
#connectstring2=global_var_data['patrick_rds1_password_format2']

def connect_and_send():
    try:
        conn = psycopg2.connect(connectstring)
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    # Auto commit is very important
    conn.set_session(autocommit=True)

    # SQLAlchemy Engine erstellen, funktioniert leider nicht
    #engine = create_engine('patrick_rds1_password_format2')
    #dataframe.to_sql('velospot', engine, if_exists='trunacte', index=False)

    cur.execute("CREATE TABLE IF NOT EXISTS velospot_all_records (geometry_x decimal, geometry_y decimal, attributes_id text, attributes_station_name text, attributes_station_status_installed text, attributes_station_status_renting text, attributes_station_status_returning text, attributes_station_status_num_vehicle_available int, featureId text, id text, timestamp text);")

    #load csv
    dataframe = pd.read_csv('/home/ubuntu/airflow/velospot_dataframe.csv')

    # Daten aus dem DataFrame in eine Liste von Tupeln konvertieren
    data = [
        (row['geometry_x'],
         row['geometry_y'],
         row['attributes_id'],
         row['attributes_station_name'],
         row['attributes_station_status_installed'],
         row['attributes_station_status_renting'],
         row['attributes_station_status_returning'],
         row['attributes_station_status_num_vehicle_available'],
         row['featureId'],
         row['id'],
         row['timestamp'])
        for index, row in dataframe.iterrows()
    ]

    # Bulk-Einfügemethode verwenden, um die Daten einzufügen
    cur.executemany("""
        INSERT INTO velospot_all_records (
            geometry_x,
            geometry_y,
            attributes_id,
            attributes_station_name,
            attributes_station_status_installed,
            attributes_station_status_renting,
            attributes_station_status_returning,
            attributes_station_status_num_vehicle_available,
            featureId,
            id,
            timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, data)

    cur.execute("SELECT COUNT(*) FROM velospot_all_records;")
    print(cur.fetchall())

    # Verbindung schließen
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
    'retries': 1
}



with DAG('velospot_send_to_rds_many', default_args=default_args, schedule_interval=None) as dag:
    connect_and_send_task = PythonOperator(
        task_id='connect_and_send_task',
        python_callable=connect_and_send
    )

if __name__ == "__main__":
    dag.cli()
