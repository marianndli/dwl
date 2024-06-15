# More than just Trains: Shared Mobility as a Complement to Swiss Public Transportation
 

This repository contains various scripts and SQL files for data processing and analysis related to public transport interruptions and Velospot bike availability.

## Table of Contents

- [DAGs](#dags)
- [Lambda Functions](#lambda-functions)
- [PostgreSQL Scripts](#postgresql-scripts)
- [Getting Started](#getting-started)
- [Requirements](#requirements)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## DAGs

- `dag4.py`: Downloads files from S3 bucket.
- `dag5.py`: Processes JSON files and loads data into PostgreSQL.
- `dag6.py`: Downloads and cleans file names from another S3 bucket.
- `dag7.py`: Combines and transforms data from JSON files.
- `dag9.py`: Loads transformed data into PostgreSQL.

## Lambda Functions

- `get_relevant_velospot_station_names.py`: Retrieves relevant Velospot station names.
- `Lambda_Interruptions.py`: Handles interruptions in public transport data.
- `Lambda_Velospot.py`: Analyzes Velospot bike availability.

## PostgreSQL Scripts

- `data_join.sql`: Performs data joining and aggregation.
- `Data_prep_interruptions.sql`: Prepares interruption data for analysis.
- `Data_prep_velospots.sql`: Prepares Velospot station data for analysis.
- `data_velospots_per_trainstation.sql`: Analyzes Velospots per train station.
- `Joined_data_analysis.sql`: Analyzes joined data from interruptions and Velospot availability.
- `Load_trainstations_to_RDS.sql`: Loads train station data into PostgreSQL RDS.

1. **Configure AWS Lambda**:
   - Ensure Lambda functions have necessary permissions for S3 and PostgreSQL RDS.
   - Lambda functions located in `/lambda_functions`:
     - `get_relevant_velospot_station_names.py`: Retrieves relevant Velospot station names.
     - `Lambda_Interruptions.py`: Lambda function for interruptions processing.
     - `Lambda_Velospot.py`: Lambda function for Velospot processing.

2. **Configure Airflow**:
   - Install and configure Apache Airflow.
   - Import DAGs located in `/dags`:
     - `dag4.py`: Downloads files from S3 bucket.
     - `dag5.py`: Processes JSON files and loads data into PostgreSQL.
     - `dag6.py`: Downloads files from another S3 bucket and cleans filenames.
     - `dag7.py`: Combines and transforms data from JSON files.
     - `dag9.py`: Loads transformed data into PostgreSQL.

3. **Configure PostgreSQL**:
   - Set up your PostgreSQL database.
   - Adjust connection details in PostgreSQL scripts as needed.
   - Files located in `/postgre_SQL`:
     - `data_join.sql`: Data joining and analysis.
     - `Data_prep_interruptions.sql`: Data preparation for interruptions analysis.
     - `Data_prep_velospots.sql`: Data preparation for Velospots analysis.
     - `data_velospots_per_trainstation.sql`: Velospots analysis per train station.
     - `Joined_data_analysis.sql`: Analysis of joined data.
     - `Load_trainstations_to_RDS.sql`: Loading train stations into RDS.

## Requirements

- PostgreSQL
- Python 3
- Apache Airflow
- AWS Lambda 

## Acknowledgments

Inspired by the need to understand the impact of public transport interruptions on alternative transportation availability.
