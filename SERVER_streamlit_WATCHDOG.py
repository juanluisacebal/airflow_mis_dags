from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import subprocess
import socket
import os
import glob
import logging

# --------------------------------------------
# DAG Default Arguments
# --------------------------------------------
default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))


ruta_streamlit = Variable.get("ruta_streamlit")

def is_port_open(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(('localhost', port)) == 0

def start_streamlit_if_needed_factory(app_file, app_path, port, log_path):
    def start_streamlit_if_needed():
        if is_port_open(port):
            with open(log_path, "a") as f:
                f.write(f"✅ Streamlit {app_file} ya está corriendo en el puerto {port}\n")
            return
        subprocess.Popen([
            "/opt/streamlit_venv/bin/streamlit", "run", app_path,
            "--server.port", str(port),
            "--server.headless", "true"
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(log_path, "a") as f:
            f.write(f"🚀 Streamlit {app_file} se lanzó automáticamente en el puerto {port}\n")
    return start_streamlit_if_needed

def get_app_number(filename):
    """Extrae el número de la app desde el nombre del archivo: app1.py → 1"""
    return int(''.join(filter(str.isdigit, filename)))

def make_task(app_file, app_path, app_num, port, log_path):
    return PythonOperator(
        task_id=f"check_streamlit_status_app{app_num}",
        python_callable=start_streamlit_if_needed_factory(app_file, app_path, port, log_path),
    )

with DAG(
    dag_id="SERVER_streamlit_WATCHDOG",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    #schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    tags=["monitoring", "streamlit"],
    doc_md="""
### DAG: SERVER_streamlit_WATCHDOG

This DAG monitors multiple Streamlit apps and ensures each one is running on its designated port.
If a Streamlit app is not reachable, it will be automatically restarted in headless mode.
"""
) as dag:
    for path in glob.glob(os.path.join(ruta_streamlit, "app*.py")):
        APP_FILE = os.path.basename(path)
        APP_PATH = os.path.join(ruta_streamlit, APP_FILE)
        APP_NUM = get_app_number(APP_FILE)
        PORT = 8700 + APP_NUM
        LOG_PATH = os.path.join(ruta_streamlit, "logs", f"streamlit_watchdog_app{APP_NUM}.log")
        task = make_task(APP_FILE, APP_PATH, APP_NUM, PORT, LOG_PATH)