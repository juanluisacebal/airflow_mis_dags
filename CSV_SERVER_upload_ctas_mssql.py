from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import csv
import pyodbc

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))


def load_csv_and_create_table():
    """
    Loads data from a CSV file and performs the following:
    1. Creates a staging table in MSSQL.
    2. Loads data from the CSV file into the staging table.
    3. Creates the final table using SELECT INTO from the staging table.
    """
    file_path = Variable.get("ruta_files")
    # Get connection from Airflow Connections
    hook = OdbcHook(odbc_conn_id='mssql', driver='FreeTDS')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # 1. Create staging table
    cursor.execute("""
        IF OBJECT_ID('stg_mi_archivo', 'U') IS NOT NULL
            DROP TABLE stg_mi_archivo;
        CREATE TABLE stg_mi_archivo (
            codigo_pais NVARCHAR(50),
            anio INT,
            valor FLOAT
        )
    """)
    conn.commit()

    # 2. Insert data from CSV
    with open(f"{file_path}/wvs_dataset.csv", newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header
        rows = []
        for row in reader:
            codigo_pais = row[0][:50]  # Truncate if too long
            anio = int(float(row[1]))  # Convert to int, tolerating values like '1998.0'
            valor = float(row[2])
            rows.append((codigo_pais, anio, valor))
        cursor.executemany("INSERT INTO stg_mi_archivo VALUES (?, ?, ?)", rows)
    conn.commit()

    # 3. Create final table using SELECT INTO
    create_query = """
    IF OBJECT_ID('mi_tabla_final', 'U') IS NOT NULL
        DROP TABLE mi_tabla_final;

    SELECT * INTO mi_tabla_final FROM stg_mi_archivo;
    """
    hook.run(create_query)



with DAG(
    dag_id='CSV_SERVER_upload_ctas_mssql',
    default_args=default_args,
    #schedule_interval=default_args["schedule_interval"],
    schedule_interval='15 5 * * *',

    #schedule_interval=None,
    tags=['mssql', 'csv', 'ctas'],
) as dag:

    upload_and_ctas = PythonOperator(
        task_id='load_csv_and_create_table',
        python_callable=load_csv_and_create_table
    )