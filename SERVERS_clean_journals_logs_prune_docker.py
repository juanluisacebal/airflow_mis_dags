from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# --------------------------------------------
# DAG Default Arguments
# --------------------------------------------
default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))



with DAG(
    dag_id='SERVERS_clean_journals_logs_prune_docker',
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    description='Clean journal logs locally and remotely',
    #schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    tags=['maintenance', 'logs'],
) as dag:

    LOCAL_log_journal_size = BashOperator(
        task_id='LOCAL_log_journal_size',
        bash_command='echo "[LOCAL] Journal size:" && journalctl --disk-usage',
    )

    LOCAL_clean_journal = BashOperator(
        task_id='LOCAL_clean_journal',
        bash_command='journalctl --vacuum-time=30d',
    )

    REMOTE_S1_log_journal_size = BashOperator(
        task_id='REMOTE_S1_log_journal_size',
        bash_command='ssh s1 "echo [remote_s1] Journal size: && journalctl --disk-usage"',
    )

    REMOTE_S1_clean_journal = BashOperator(
        task_id='REMOTE_S1_clean_journal',
        bash_command='ssh s1 "journalctl --vacuum-time=7d"',
    )

    LOCAL_docker_prune_and_log_freed_space = BashOperator(
        task_id='LOCAL_docker_prune_and_log_freed_space',
        bash_command='''
        echo "📦 Docker usage before:"
        BEFORE=$(docker system df -v | tee /tmp/docker_before.txt | grep "Total space used" | awk '{print $4, $5}')
        docker system prune -af --volumes
        echo "📦 Docker usage after:"
        AFTER=$(docker system df -v | tee /tmp/docker_after.txt | grep "Total space used" | awk '{print $4, $5}')
        echo "🧹 Freed space: $BEFORE -> $AFTER"
        ''',
    )

    LOCAL_log_journal_size >> LOCAL_clean_journal >> LOCAL_docker_prune_and_log_freed_space
    REMOTE_S1_log_journal_size >> REMOTE_S1_clean_journal
    [LOCAL_docker_prune_and_log_freed_space, REMOTE_S1_clean_journal] 