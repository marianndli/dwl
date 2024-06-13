import csv
import psycopg2

# PostgreSQL RDS connection parameters
rds_host = "interruptions.cgd1asugso5g.us-east-1.rds.amazonaws.com"
db_name = "interruptions"
username = "kira"
password = "interruptions"
port = "5432"  # Usually 5432 for PostgreSQL

# Path to your local CSV file
csv_file_path = 'C:/Users/Kira/OneDrive - Hochschule Luzern/Data Warehouse/Unterlagen Patrick/Velospot Stationen/relevant_velospotstationscsv.csv'

try:
    # Establish the PostgreSQL connection
    connection = psycopg2.connect(
        host=rds_host,
        dbname=db_name,
        user=username,
        password=password,
        port=port
    )
    cursor = connection.cursor()

    # Open the CSV file
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        
        # Skip the header row (if your CSV file has a header)
        next(csv_reader)

        # Prepare and execute the insert query for each row in the CSV
        for row in csv_reader:
            # Adapt the SQL insert statement to your table structure
            insert_query = """
            INSERT INTO trainstations (station_name, count_velospot33, count_velospotname33) VALUES (%s, %s, %s)
            """
            cursor.execute(insert_query, (row[0], row[1], row[2]))

    # Commit the transaction
    connection.commit()

    print('CSV data successfully saved to RDS')

except Exception as e:
    print(f'Error: {str(e)}')

finally:
    # Ensure the database connection is closed
    if connection:
        cursor.close()
        connection.close()
