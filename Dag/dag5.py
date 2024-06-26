from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from airflow.hooks.base_hook import BaseHook
import yaml
import os
import pandas as pd
import psycopg2
import json
import csv

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


def get_jsons_and_prep():
    combined_data = []
    folder_path='/home/ubuntu/airflow/interruptions'

    files = os.listdir(folder_path)

    for file in files:
        with open(os.path.join(folder_path, file), 'r') as filetoopen:
            data = json.load(filetoopen)
            results = data.get("results", [])  # Hier wird der Wert von "results" geholt, falls vorhanden, oder eine leere Liste verwendet
            combined_data.extend(results)  # Hier werden die Ergebnisse der kombinierten Liste hinzugefügt

    df = pd.DataFrame(combined_data)

    relevant_stations = [
        "Locarno",
        "Locarno S. Antonio",
        "Solduno",
        "Solduno S. Martino",
        "Ponte Brolla",
        "Riazzino",
        "Intragna",
        "Cavigliano",
        "Tegna",
        "Minusio",
        "Tenero",
        "Gordola",
        "Cadenazzo",
        "Quartino",
        "Giubiasco",
        "La Chaux-de-Fonds",
        "Martigny-Bourg",
        "Martigny",
        "Vevey-Funi",
        "Vevey",
        "La Tour-de-Peilz",
        "Burier",
        "Montreux",
        "Biel/Bienne",
        "Biel/Bienne Bözingenfeld/Champ",
        "Bellinzona",
        "S. Antonino",
        "Villeneuve VD",
        "Siggenthal-Würenlingen",
        "Aigle-Place-du-Marché",
        "Aigle",
        "Basel SBB",
        "Basel Bad Bf",
        "Martigny-Expo",
        "Basel St. Jakob",
        "Ardon",
        "Châteauneuf-Conthey",
        "Salgesch",
        "Basel St. Johann"
    ]

    df_rel = df[df['haltestellen_name'].isin(relevant_stations)].copy()
    df_rel.reset_index(drop=True, inplace=True)
    # Spalte 'geopos' in 'lon' und 'lat' aufteilen
    df_rel[['lon', 'lat']] = pd.DataFrame(df_rel['geopos'].tolist())
    df_rel.drop('geopos', axis=1, inplace=True)
    print("df_rel created")
    print(len(df_rel))
    #save as csv as it is not possible to use the df_rel direct in the other dag
    df_rel.to_csv('/home/ubuntu/airflow/interruptions_df_rel.csv', index=False)

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

    cur.execute("DROP TABLE IF EXISTS rel_interruptions2;")
    cur.execute("CREATE TABLE IF NOT EXISTS rel_interruptions2 (betriebstag text, fahrt_bezeichner text, betreiber_id text, betreiber_abk text, betreiber_name text, produkt_id text, linien_id text, linien_text text, verkehrsmittel_text text, zusatzfahrt_tf text, faellt_aus_tf text, bpuic text, haltestellen_name text, ankunftszeit text, an_prognose text, an_prognose_status text, abfahrtszeit text, ab_prognose text, ab_prognose_status text, durchfahrt_tf text, ankunftsverspatung text, abfahrtsverspatung text, lon decimal, lat decimal);")

    #load csv
    df_rel = pd.read_csv('/home/ubuntu/airflow/interruptions_df_rel.csv')

    for index, row in df_rel.iterrows():
        cur.execute("""
            INSERT INTO rel_interruptions2 (
                betriebstag,
                fahrt_bezeichner,
                betreiber_id,
                betreiber_abk,
                betreiber_name,
                produkt_id,
                linien_id,
                linien_text,
                verkehrsmittel_text,
                zusatzfahrt_tf,
                faellt_aus_tf,
                bpuic,
                haltestellen_name,
                ankunftszeit,
                an_prognose,
                an_prognose_status,
                abfahrtszeit,
                ab_prognose,
                ab_prognose_status,
                durchfahrt_tf,
                ankunftsverspatung,
                abfahrtsverspatung,
                lon,
                lat
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            row['betriebstag'],
            row['fahrt_bezeichner'],
            row['betreiber_id'],
            row['betreiber_abk'],
            row['betreiber_name'],
            row['produkt_id'],
            row['linien_id'],
            row['linien_text'],
            row['verkehrsmittel_text'],
            row['zusatzfahrt_tf'],
            row['faellt_aus_tf'],
            row['bpuic'],
            row['haltestellen_name'],
            row['ankunftszeit'],
            row['an_prognose'],
            row['an_prognose_status'],
            row['abfahrtszeit'],
            row['ab_prognose'],
            row['ab_prognose_status'],
            row['durchfahrt_tf'],
            row['ankunftsverspatung'],
            row['abfahrtsverspatung'],
            row['lon'],
            row['lat']
        ))

    cur.execute("SELECT * FROM rel_interruptions2 LIMIT 10;")
    print(cur.fetchall())

    cur.execute("SELECT COUNT(*) FROM rel_interruptions2;")
    print(cur.fetchall())

    cur.execute("SELECT COUNT(*) FROM rel_interruptions2 WHERE haltestellen_name = 'Burier'")
    print(cur.fetchall())

    cur.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
    'retries': 1
}

with DAG('interruptions_prep_and_send_to_rds', default_args=default_args, schedule_interval=None) as dag:
    access_s3_task = PythonOperator(
        task_id='get_jsons_and_prep',
        python_callable=get_jsons_and_prep
    )

    connect_send_task = PythonOperator(
        task_id='connect_and_send_to_rds',
        python_callable=connect_and_send
    )

    # Definiere die Aufgabenreihenfolge und Abhängigkeiten
    access_s3_task >> connect_send_task


if __name__ == "__main__":
    dag.cli()
