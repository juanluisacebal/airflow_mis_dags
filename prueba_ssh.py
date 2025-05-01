from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime



def ssh_test():
    import subprocess
    result = subprocess.run(["ssh", "s2prueba", "echo DAG_OK"], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)
    result.check_returncode()

with DAG(
    dag_id="prueba_ssh",
    start_date=datetime(2025, 4, 30),
    schedule_interval=None,  # o "@once" para ejecutarlo manualmente
    catchup=False,
    tags=["test", "ssh"],
) as dag:

    test_ssh_task = PythonOperator(
        task_id="test_ssh_from_dag",
        python_callable=ssh_test,
    )