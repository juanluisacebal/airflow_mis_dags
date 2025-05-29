from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import csv
import psycopg2
import re
from airflow.models import Variable
import os
import subprocess

ruta_files = Variable.get("ruta_files")
default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

def backup_git_repos(**kwargs):
    backup_git = Variable.get("backup_git", deserialize_json=True)
    if not backup_git:
        print("📭 Repository list is empty.")
        return
    current_repo = backup_git.pop(0)

    repo_path = current_repo
    # If the variable points to the *.git* directory, move up to the repo root
    if repo_path.rstrip(os.sep).endswith(os.sep + '.git'):
        repo_path = os.path.dirname(repo_path)
    if not os.path.isdir(repo_path):
        return

    try:
        try:
            subprocess.run(["chmod", "-R", "g+rX", repo_path], check=True)
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Error changing permissions in {repo_path}: {e}")

        precommit_script = os.path.join(repo_path, "precommit.sh")
        ruta_backup = Variable.get("RUTA_BACKUP_DATAHUB")
        if os.path.isfile(precommit_script):
            print(f"🔧 Running precommit.sh in {repo_path}...")
            try:
                result = subprocess.run(
                    ["bash", precommit_script],
                    capture_output=True,
                    text=True,
                    check=True,
                    env={**os.environ, "RUTA_BACKUP_DATAHUB": ruta_backup},
                    cwd=repo_path
                )
                print(f"📤 Output from precommit.sh in {repo_path}:\n{result.stdout}")
                if result.stderr.strip():
                    print(f"❌ Errors from precommit.sh in {repo_path}:\n{result.stderr}")
            except subprocess.CalledProcessError as e:
                print(f"❗ Error running precommit.sh in {repo_path}: {e}")
                print(f"📤 STDOUT:\n{e.stdout}")
                print(f"❌ STDERR:\n{e.stderr}")

        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, cwd=repo_path)
        #if result.stdout.strip() != "":
        #    print(f"✅ No changes to commit in {repo_path}")
        if True:
            try:
                result = subprocess.run(["git", "pull"], capture_output=True, text=True, check=True, cwd=repo_path)
                print(f"📥 Output from git pull in {repo_path}:\n{result.stdout}")
                if result.stderr:
                    print(f"⚠️ Errors from git pull in {repo_path}:\n{result.stderr}")
            except subprocess.CalledProcessError as e:
                print(f"❗ Error running git pull in {repo_path}: {e}")
                print(f"📤 STDOUT:\n{e.stdout}")
                print(f"❌ STDERR:\n{e.stderr}")

            result = subprocess.run(["git", "add", "."], capture_output=True, text=True, check=True, cwd=repo_path)
            print(f"📥 Output from git add in {repo_path}:\n{result.stdout}")
            if result.stderr:
                print(f"⚠️ Errors from git add in {repo_path}:\n{result.stderr}")

            fecha_commit = datetime.now().strftime("%d-%m-%y")
            result = subprocess.run(["git", "commit", "-m", f"Commit {fecha_commit}"], capture_output=True, text=True, check=True, cwd=repo_path)
            print(f"📝 Output from git commit in {repo_path}:\n{result.stdout}")
            if result.stderr:
                print(f"⚠️ Errors from git commit in {repo_path}:\n{result.stderr}")
            result = subprocess.run(["git", "push"], capture_output=True, text=True, check=True, cwd=repo_path)
            print(f"🚀 Output from git push in {repo_path}:\n{result.stdout}")
            if result.stderr:
                print(f"⚠️ Errors from git push in {repo_path}:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"❗ Error during commit/push in {repo_path}: {e}")

    backup_git.append(current_repo)
    Variable.set("backup_git", backup_git, serialize_json=True)

"""
🗂️ **DAG: SERVER_GIT_backup**

🔄 This DAG automates the backup process for a set of local Git repositories. It performs the following operations for each repository path defined in the `backup_git` Airflow Variable:

📁 Steps:
- 📂 Navigates to the repository directory.
- 🛠️ Runs `precommit.sh` if present.
- ➕ Stages changes with `git add .`.
- 🔍 Checks status with `git status --porcelain`.
- 📝 Commits with a timestamp.
- 🚀 Pushes to remote repository.
- 🔁 Cycles through repository list using the `backup_git` variable.

🗓️ Schedule: Monday to Friday at 07:00 AM  
📌 Tags: GIT, BACKUP

🧩 **Airflow Variables used:**
- `ruta_files`: Optional base directory.
- `default_args`: Dict with DAG config.
- `backup_git`: JSON list of repo paths.
- `RUTA_BACKUP_DATAHUB`: Used by `precommit.sh`.
"""

with DAG(
    dag_id='SERVER_GIT_backup',
    default_args=default_args,
    #schedule_interval=default_args["schedule_interval"],
    schedule_interval="0,10 7 * * 1-5",
    tags=['GIT', 'BACKUP']
) as dag_SERV_GIT_BACKUP:
    backup_git_task = PythonOperator(
        task_id="backup_git_repos",
        python_callable=backup_git_repos,
        provide_context=True,
        doc_md="""
        🧩 **Task: backup_git_repos**

        Performs the backup of a single repository path by:
        - 🧭 Navigating to the path.
        - 🛠️ Running optional `precommit.sh`.
        - 📦 Staging and committing changes.
        - 🚀 Pushing to remote repository.
        - 🔁 Updating the `backup_git` list.
        """
    )
