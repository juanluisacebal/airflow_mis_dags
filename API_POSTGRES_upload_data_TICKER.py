import pandas as pd
import yfinance as yf
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from airflow.models import DagRun, Variable
from airflow.utils.trigger_rule import TriggerRule
import logging
import psycopg2

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

ruta_files = Variable.get("ruta_files")

def extract_all_tickers(**kwargs):
    for ticker in tickers:
        logging.info(f"📥 Starting data extraction for ticker: {ticker}")
        try:
            df = yf.download(ticker, start="1900-01-01", interval="1d")
            if df.empty:
                logging.warning(f"⚠️ No data downloaded for ticker {ticker}")
            else:
                logging.info(f"📊 DataFrame shape for {ticker}: {df.shape}")
            kwargs["ti"].xcom_push(key=f"{ticker}_raw_df", value=df.to_json())
        except Exception as e:
            logging.error(f"❌ Error downloading data for {ticker}: {e}")
            raise

def transform_all_tickers(**kwargs):
    ti = kwargs["ti"]
    for ticker in tickers:
        raw_json = ti.xcom_pull(key=f"{ticker}_raw_df")
        if not raw_json:
            logging.warning(f"⚠️ No raw data found for {ticker}")
            continue
        df = pd.read_json(raw_json)
        df.reset_index(inplace=True)
        df["ticker"] = ticker
        csv_path = f"{ruta_files}/temp/{ticker}_transformed.csv"
        df.to_csv(csv_path, index=False)
        ti.xcom_push(key=f"{ticker}_csv_path", value=csv_path)
        logging.info(f"🔄 Transformed and saved data for {ticker} to {csv_path}")

def load_all_tickers(**kwargs):
    ti = kwargs["ti"]
    logging.info(f"🔄 Loading all tickers")
    
    conn = BaseHook.get_connection("postgres_default")
    connection = psycopg2.connect(
        host=conn.host,
        database=conn.schema,
        user=conn.login,
        password=conn.password,
        port=conn.port
    )
    cursor = connection.cursor()
    
    try:
        # Crear schema stage si no existe
        cursor.execute("CREATE SCHEMA IF NOT EXISTS stage")
        connection.commit()
        logging.info("✅ Schema 'stage' verificado/creado")
        
        for ticker in tickers:
            logging.info(f"🔄 Loading {ticker}")
            csv_path = ti.xcom_pull(key=f"{ticker}_csv_path")
            logging.info(f"🔄 CSV path: {csv_path}")
            
            if not csv_path:
                logging.warning(f"⚠️ No transformed CSV path found for {ticker}")
                continue
                
            df = pd.read_csv(csv_path)
            logging.info(f"🔄 DataFrame shape: {df.shape}")
            
            # Limpiar nombres de columnas
            df.columns = [col.replace("('", "").replace("')", "").replace(", '", "_").replace("'", "").lower() for col in df.columns]
            
            # Detectar columnas automáticamente
            date_col = next((col for col in df.columns if col in ['index', 'date']), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
            
            # Detectar y convertir columnas numéricas individualmente
            for col in df.columns:
                if any(pattern in col for pattern in ['close', 'high', 'low', 'open']):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Detectar y convertir columna de volumen
            volume_col = next((col for col in df.columns if 'volume' in col), None)
            if volume_col:
                df[volume_col] = pd.to_numeric(df[volume_col], errors='coerce').fillna(0).astype('int64')
            
            table_name = f"stage.stg_{ticker.lower()}"
            try:
                logging.info(f"🚀 Uploading {ticker} to PostgreSQL")
                
                # Crear definición de columnas dinámicamente
                columns_def = []
                for col in df.columns:
                    if col == date_col:
                        columns_def.append(f"{col} TIMESTAMP")
                    elif col == 'ticker':
                        columns_def.append(f"{col} VARCHAR(10)")
                    elif col == volume_col:
                        columns_def.append(f"{col} BIGINT")
                    else:
                        columns_def.append(f"{col} DOUBLE PRECISION")
                
                # Drop y Create de la tabla staging
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                create_stmt = f"CREATE TABLE {table_name} ({', '.join(columns_def)})"
                cursor.execute(create_stmt)
                connection.commit()
                
                # Preparar datos para inserción
                df_cleaned = df.where(pd.notnull(df), None)
                records = [tuple(row) for row in df_cleaned.itertuples(index=False, name=None)]
                
                # Insertar datos en batches
                batch_size = 500
                placeholders = ','.join(['%s' for _ in df.columns])
                insert_stmt = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
                
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    cursor.executemany(insert_stmt, batch)
                    connection.commit()
                    logging.info(f"✅ Uploaded batch {i//batch_size + 1} of {len(records)//batch_size} for {ticker}")
                
                logging.info(f"✅ Successfully loaded {len(df)} records for {ticker}")
                
            except Exception as e:
                logging.error(f"❌ Failed to load {ticker}: {e}")
                connection.rollback()
                raise
    except Exception as e:
        logging.error(f"❌ Error general en load_all_tickers: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

def update_final_table(**kwargs):
    """
    Asegura que el schema existe y actualiza finance.tickers_historicos con nuevos registros de cada tabla staging
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

    try:
        # Crear schema si no existe
        cursor.execute("CREATE SCHEMA IF NOT EXISTS finance")
        connection.commit()

        # Primero, crear la tabla final si no existe con una estructura fija
        final_table = "finance.tickers_historicos"
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {final_table} (
                date TIMESTAMP,
                close DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                open DOUBLE PRECISION,
                volume BIGINT,
                ticker VARCHAR(10)
            )
        """)
        connection.commit()

        for ticker in tickers:
            staging_table = f"stage.stg_{ticker.lower()}"
            
            # Verificar si la tabla staging existe
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'stage' 
                    AND table_name = %s
                )
            """, (f"stg_{ticker.lower()}",))
            
            if not cursor.fetchone()[0]:
                logging.warning(f"⚠️ Tabla staging {staging_table} no existe")
                continue

            # Obtener los nombres de columnas de la tabla staging
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'stage' 
                AND table_name = 'stg_{ticker.lower()}'
            """)
            staging_columns = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Identificar columnas clave en staging
            date_col = next((col for col in staging_columns.keys() if col in ['index', 'date']), None)
            close_col = next((col for col in staging_columns.keys() if 'close' in col), None)
            high_col = next((col for col in staging_columns.keys() if 'high' in col), None)
            low_col = next((col for col in staging_columns.keys() if 'low' in col), None)
            open_col = next((col for col in staging_columns.keys() if 'open' in col), None)
            volume_col = next((col for col in staging_columns.keys() if 'volume' in col), None)

            if not all([date_col, close_col, high_col, low_col, open_col, volume_col]):
                logging.warning(f"⚠️ No se encontraron todas las columnas necesarias en {staging_table}")
                continue

            # Obtener fecha máxima para el ticker de la tabla final
            cursor.execute("""
                SELECT COALESCE(MAX(date), '1900-01-01'::timestamp) 
                FROM finance.tickers_historicos 
                WHERE ticker = %s
            """, (ticker,))
            max_date = cursor.fetchone()[0]

            logging.info(f"🧮 Max date in final table for {ticker}: {max_date}")

            # Insertar nuevos registros mapeando las columnas correctamente
            insert_sql = f"""
                INSERT INTO {final_table} (date, close, high, low, open, volume, ticker)
                SELECT 
                    s.{date_col}::timestamp, 
                    s.{close_col}::double precision,
                    s.{high_col}::double precision,
                    s.{low_col}::double precision,
                    s.{open_col}::double precision,
                    s.{volume_col}::bigint,
                    s.ticker
                FROM {staging_table} s
                WHERE s.{date_col} > %s
            """
            cursor.execute(insert_sql, (max_date,))
            rows_inserted = cursor.rowcount
            connection.commit()
            logging.info(f"📥 Rows inserted for {ticker}: {rows_inserted}")

    except Exception as e:
        logging.error(f"❌ Error updating final table: {str(e)}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()

# --------------------------------------------
# DAG Definition
# --------------------------------------------
with DAG(
    dag_id="API_POSTGRES_upload_data_TICKER",
    default_args=default_args,
    #schedule_interval=default_args["schedule_interval"],
    schedule_interval='15 4 * * *',
    tags=["API", "PostgreSQL", "Finance", "ETL"],
    doc_md="""
### DAG: Upload Ticker Historical Data to PostgreSQL

Este DAG extrae datos históricos de Yahoo Finance para los tickers seleccionados,
y los carga en una tabla staging (`stage.stg_ticker`) en PostgreSQL.

Todos los tickers son procesados secuencialmente usando tareas consolidadas.
"""
) as dag:

    tickers = Variable.get("tickers", default_var='["SPY", "QQQ", "DIA"]', deserialize_json=True)
    tickers = [t.strip() for t in tickers if t.strip().isalnum()]
    ticker_dataset = Dataset("postgres://postgres:airflow@localhost:5432/airflow/finance/tickers_historicos")

    extract_all = PythonOperator(
        task_id="extract_all_tickers",
        python_callable=extract_all_tickers,
        provide_context=True
    )

    transform_all = PythonOperator(
        task_id="transform_all_tickers",
        python_callable=transform_all_tickers,
        provide_context=True
    )

    load_all = PythonOperator(
        task_id="load_all_tickers",
        python_callable=load_all_tickers,
        provide_context=True,
        outlets=[ticker_dataset]
    )

    update_final = PythonOperator(
        task_id="update_final_table",
        python_callable=update_final_table,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="""
#### Task: update_final_table

Esta tarea asegura que exista el schema `finance` y la tabla `tickers_historicos`.
Fusiona datos de cada tabla staging insertando solo nuevos registros en la tabla final.
"""
    )

    extract_all >> transform_all >> load_all >> update_final

    def validate_tickers_exist(**kwargs):
        """
        Valida los tickers verificando su existencia en Yahoo Finance.
        Elimina cualquier ticker inválido y actualiza la Variable de Airflow.
        """
        from yfinance import Ticker

        valid_tickers = []
        invalid_tickers = []

        for ticker in tickers:
            try:
                info = Ticker(ticker).info
                if info and info.get("regularMarketPrice") is not None:
                    valid_tickers.append(ticker)
                else:
                    invalid_tickers.append(ticker)
            except Exception as e:
                logging.warning(f"⚠️ Ticker {ticker} is invalid or could not be validated: {e}")
                invalid_tickers.append(ticker)

        if invalid_tickers:
            logging.warning(f"🚫 Invalid tickers from Yahoo Finance: {invalid_tickers}")
            Variable.set("tickers", valid_tickers, serialize_json=True)
            Variable.set_description("tickers", f"Removed invalid tickers: {invalid_tickers}")

    validate_tickers = PythonOperator(
        task_id="validate_tickers",
        python_callable=validate_tickers_exist,
        provide_context=True,
        doc_md="""
#### Task: validate_tickers

Esta tarea verifica si las tablas staging existen para cada ticker.
Si no, el ticker es eliminado de la variable `tickers` y se anota en la descripción.
"""
    )

    validate_tickers >> extract_all
