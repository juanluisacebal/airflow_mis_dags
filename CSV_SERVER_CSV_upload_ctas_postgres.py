from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import csv
import psycopg2
import re
from airflow.models import Variable



file_path = Variable.get("ruta_files")
default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

def load_csv_and_create_table_postgres():
    """
    Loads data from a CSV file and performs the following in PostgreSQL:
    1. Creates a staging table.
    2. Inserts data into the staging table.
    3. Creates the final table using a SELECT INTO from the staging table.
    """
    conn = BaseHook.get_connection("postgres_default")
    connection = psycopg2.connect(
        host=conn.host,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()

    # 1. Create staging table
    cursor.execute("""
        DROP TABLE IF EXISTS stg_my_file_VDD_PEC3;
        CREATE TABLE stg_my_file_VDD_PEC3 (
            codigo TEXT,
            pais TEXT,
            anio INT,
            tradition_secular_axis FLOAT,
            survival_selfexpression_axis FLOAT
        )
    """)
    connection.commit()

    # 2. Insert data from CSV
    with open(f"{file_path}/wvs_dataset.csv", newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        rows = []
        for row in reader:
            match = re.match(r"(\d+)\s+(.*?)\s+\((\d{4})\)", row[0])
            if not match:
                continue
            codigo = match.group(1)
            pais = match.group(2)
            anio = int(match.group(3))
            tradition = float(row[1])
            survival = float(row[2])
            rows.append((codigo, pais, anio, tradition, survival))
        cursor.executemany("INSERT INTO stg_my_file_VDD_PEC3 VALUES (%s, %s, %s, %s, %s)", rows)
    connection.commit()

    # 3. Create final table using SELECT INTO
    cursor.execute("""
        DROP TABLE IF EXISTS Data_Visualization_PEC3;
        CREATE TABLE Data_Visualization_PEC3 AS SELECT * FROM stg_my_file_VDD_PEC3;
    """)
    connection.commit()
    connection.close()

def export_postgres_table_to_csv():
    """
    Exports the final PostgreSQL table to a CSV file with headers.
    Converts float values to use commas as decimal separators.
    """
    conn = BaseHook.get_connection("postgres_default")
    connection = psycopg2.connect(
        host=conn.host,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM Data_Visualization_PEC3")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    with open(f"{file_path}/output_resultado.csv", 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)

        formatted_rows = []
        for row in rows:
            formatted_row = []
            for value in row:
                if isinstance(value, float):
                    formatted_value = str(value).replace('.', ',')
                    formatted_row.append(formatted_value)
                else:
                    formatted_row.append(value)
            formatted_rows.append(formatted_row)

        writer.writerows(formatted_rows)

    connection.close()

with DAG(
    dag_id='CSV_SERVER_CSV_upload_ctas_postgres',
    default_args=default_args,
    #schedule_interval=default_args["schedule_interval"],
    schedule_interval='15 6 * * *',
    #schedule_interval=None,
    tags=['postgres', 'csv', 'ctas'],
) as dag_postgres:

    load_csv_and_create_table_postgres = PythonOperator(
        task_id='load_csv_and_create_table_postgres',
        python_callable=load_csv_and_create_table_postgres
    )

    export_postgres_table_to_csv = PythonOperator(
        task_id='export_postgres_table_to_csv',
        python_callable=export_postgres_table_to_csv
    )

    load_csv_and_create_table_postgres >> export_postgres_table_to_csv