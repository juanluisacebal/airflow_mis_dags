from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import hashlib


#Default args
default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

notification_emails = Variable.get(
    "email_notification",
    default_var="airflow@juanluisacebal.com"
).split(",")

# Obtener la ruta desde una Variable de Airflow
ruta_airflow = Variable.get("ruta_airflow")
proyecto_path = os.path.join(ruta_airflow, "challenge_graphql_nlp_api")
hash_file = "/tmp/challenge_graphql_hash.txt"

def compute_directory_hash():
    hash_md5 = hashlib.md5()
    for root, _, files in os.walk(proyecto_path):
        for f in sorted(files):
            if f.endswith(".py"):
                path = os.path.join(root, f)
                with open(path, "rb") as file:
                    while chunk := file.read(4096):
                        hash_md5.update(chunk)
    return hash_md5.hexdigest()

def detect_changes(**context):
    new_hash = compute_directory_hash()
    old_hash = None

    if os.path.exists(hash_file):
        with open(hash_file, "r") as f:
            old_hash = f.read().strip()

    if new_hash != old_hash:
        with open(hash_file, "w") as f:
            f.write(new_hash)
        return "rebuild_docker"
    return "skip_rebuild"

with DAG(
    "SERVER_watch_challenge_graphql_api_nlp",
    #schedule_interval="1 * * * *",
    start_date=days_ago(1),
    schedule_interval=default_args["schedule_interval"],
    tags=["docker", "monitor"],
) as dag:

    check_for_changes = PythonOperator(
        task_id="check_changes",
        python_callable=detect_changes,
        provide_context=True,
    )

    rebuild_docker = BashOperator(
        task_id="rebuild_docker",
        bash_command=f"""
        cd {proyecto_path}
        docker compose -f docker/docker-compose.yml down
        docker compose -f docker/docker-compose.yml up --build 
        """,
    )#--force-recreate -d

    skip_rebuild = BashOperator(
        task_id="skip_rebuild",
        bash_command="echo 'No changes detected.'",
    )

    check_for_changes >> [rebuild_docker, skip_rebuild]