from airflow import DAG
import os
import json
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import csv



def mergevelospot():
    previous_number = None
    combined_data = []
    folder_path='/home/ubuntu/airflow/velospot/'

    files = os.listdir(folder_path)

    for file in files:
        number = int(file[11:17])
        if previous_number is not None and number - previous_number <= 60:
            with open(os.path.join(folder_path, file), 'r') as filetoopen:
                data = json.load(filetoopen)
                combined_data += data
        previous_number = number
    


    # Write combined data to a new file
    output_filename = os.path.join('/home/ubuntu/airflow/','velospot_combined_data.json')
    with open(output_filename, 'w') as output_file:
        json.dump(combined_data, output_file, indent=4)

def transformvelospot():

    # Pfad zur JSON-Datei
    json_pfad = '/home/ubuntu/airflow/velospot_combined_data.json'

    # JSON-Datei öffnen und Daten laden
    with open(json_pfad, 'r') as json_datei:
        json_objekt = json.load(json_datei)

    # Funktion zum Auflösen der Verschachtelung definieren
    def flatten_json(json_objekt, prefix=''):
        flat_dict = {}
        for key, value in json_objekt.items():
            if isinstance(value, dict):
                flat_dict.update(flatten_json(value, f"{prefix}{key}_"))
            else:
                flat_dict[f"{prefix}{key}"] = value
        return flat_dict

    # JSON-Objekte flach machen
    flache_daten = [flatten_json(item) for item in json_objekt]

    # Daten in ein flaches Dataframe konvertieren
    dataframe = pd.DataFrame(flache_daten)

    spalten_zum_loeschen = ['geometry_spatialReference_wkid', 'attributes_provider_id','attributes_provider_name', 'attributes_provider_timezone', 'attributes_available', 'attributes_pickup_type','attributes_station_region_id', 'attributes_vehicle_type', 'layerBodId','layerName' ]
    dataframe.drop(columns=spalten_zum_loeschen, inplace=True)

    print("dataframe created")
    print(len(dataframe))
    #save as csv as it is not possible to use the dataframe direct in the other dag
    dataframe.to_csv('/home/ubuntu/airflow/velospot_dataframe.csv', index=False)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 3),
    'retries': 1
}

with DAG('velospot_merge_transform', default_args=default_args, schedule_interval=None) as dag:
    mergevelospot_task = PythonOperator(
        task_id='mergevelospot_task',
        python_callable=mergevelospot
    )

    transformvelospot_task = PythonOperator(
        task_id='transformvelospot_task',
        python_callable=transformvelospot
    )

    # Definiere die Aufgabenreihenfolge und Abhängigkeiten
    mergevelospot_task >> transformvelospot_task

if __name__ == "__main__":
    dag.cli()
