# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 14:46:16 2024

@author: Patrick
"""

import requests
import json
import csv

# Basis-URL der API
base_url = "https://api.sharedmobility.ch/v1/sharedmobility/identify"

# Startparameter für die Abfrage (Vorgehen: 250km im Umkreis von Zug alle Stationen abfragen)
#somit sind alle Stationen in der Schweiz inkludiert
geometry = "8.512291781935305,47.156581137842316"
tolerance = "250000"
offset = 0  # Start-Offset
limit = 50  # Limit pro Abfrage
geometry_format = "esrijson"

# Liste zur Speicherung aller abgerufenen Daten
all_data = []

# Schleife zum Abrufen aller Daten mit einem Offset von 50 pro Abfrage
while True:
    # Aufbau der Abfrage-URL mit dem aktuellen Offset
    url = f"{base_url}?Geometry={geometry}&Tolerance={tolerance}&offset={offset}&geometryFormat={geometry_format}"

    # Senden der Anfrage
    response = requests.get(url)
    
    # Überprüfen des Statuscodes der Antwort
    if response.status_code == 200:
        # Daten von der Antwort extrahieren
        data = response.json()
        
        # Überprüfen, ob Daten vorhanden sind
        if data:
            # Hinzufügen der abgerufenen Daten zur Gesamtliste
            all_data.extend(data)
            
            # Erhöhen des Offsets für die nächste Abfrage
            offset += limit
        else:
            # Wenn keine Daten mehr vorhanden sind, die Schleife beenden
            break
    else:
        # Bei einem anderen Statuscode als 200 die Schleife beenden und eine Fehlermeldung ausgeben
        print("Fehler beim Abrufen der Daten. Statuscode:", response.status_code)
        break

# Gesamte abgerufene Daten anzeigen
print("Anzahl der abgerufenen Daten:", len(all_data))

filtered_data = [entry for entry in all_data if "station_status_num_vehicle_available" in entry["attributes"]]

# Filtern der gefilterten Daten, um nur Einträge mit provider_id = "velospot" zu erhalten
velospot_entries = [entry for entry in filtered_data if entry["attributes"].get("provider_id") == "velospot"]


##########################
# Pfad zur JSON-Datei
file_path = "dienststellen-gemass-opentransportdataswiss.json"

# JSON-Datei öffnen und einlesen
with open(file_path, "r", encoding="utf-8") as file:
    data_dienststellen = json.load(file)


#########################
# Schleife durch die Einträge in velospot_entries
for velospot_entry in velospot_entries:
    # Extrahieren der x- und y-Werte aus dem Eintrag
    x = velospot_entry["geometry"]["x"]
    y = velospot_entry["geometry"]["y"]
    
    # Schleife durch die Einträge in data_dienststellen
    for dienststelle in data_dienststellen:
        # Extrahieren der lat- und lon-Werte aus dem Eintrag
        lat = dienststelle["geopos"]["lat"]
        lon = dienststelle["geopos"]["lon"]
        stat_name = dienststelle.get("designationofficial", "")
        
        # Berechnen der Distanz zwischen den Koordinaten
        y_dist= y - lat
        x_dist= x - lon
        x_dist=((x_dist)**2)**0.5
        y_dist=((y_dist)**2)**0.5
        
        

        if x_dist <= 0.0018 and y_dist <= 0.0018:
            velospot_entry["count_velospot33"] = velospot_entry.get("count_velospot33", 0) + 1
            velospot_entry["count_velospotname33"] = velospot_entry.get("count_velospotname33", []) + [stat_name]

entries_with_count_velospotname33 = [entry for entry in velospot_entries if "count_velospotname33" in entry]


# Dateiname für die JSON-Datei
file_name_relevant_velospotstations = "relevant_velospotstationsjson.json"

# Schreiben des Objekts als JSON in die Datei
with open(file_name_relevant_velospotstations, 'w') as json_file:
    json.dump(entries_with_count_velospotname33, json_file)


# Name der CSV-Datei
csv_file_name = "relevant_velospotstationscsv.csv"

# Öffnen der CSV-Datei im Schreibmodus
with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)

    # Schreiben der Spaltenüberschriften
    writer.writerow(["station_name", "count_velospot33", "count_velospotname33"])

    # Iteration durch die JSON-Daten und Schreiben der Zeilen
    for entry in entries_with_count_velospotname33:
        station_name = entry["attributes"]["station_name"]
        count_velospot33 = entry["count_velospot33"]
        count_velospotname33 = ", ".join(entry["count_velospotname33"]) if entry["count_velospotname33"] else ""
        
        # Schreiben der Zeile in die CSV-Datei
        writer.writerow([station_name, count_velospot33, count_velospotname33])
