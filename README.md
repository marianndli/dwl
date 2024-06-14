# dwl
This project leverages AWS services, including Lambda functions, to fetch and process data from various APIs. The processed data is then orchestrated using Apache Airflow and stored in an RDS PostgreSQL database. The main goal is to integrate shared mobility solutions with Swiss Federal Railways (SBB) infrastructure to improve transportation efficiency during train disruptions.

# Data Collection with AWS Lambda:

Lambda functions (Lambda_Interruptions.py, Lambda_Velospot.py, get_relevant_velospot_station_names.py) fetch data from APIs and process it.
The processed data is stored in AWS RDS.
Orchestration with Apache Airflow:

Airflow DAGs (dag4.py, dag5.py, dag6.py, dag7.py, dag9.py) manage the workflows, including data extraction, transformation, and loading (ETL) processes.
These DAGs schedule and monitor the tasks, ensuring data is correctly processed and loaded into the PostgreSQL database.
Data Storage and Analysis in PostgreSQL:

The final processed data is stored in a PostgreSQL database.
SQL scripts (data_join.sql, Data_preparation_interruptions.sql, etc.) prepare and analyze the data for further insights.
