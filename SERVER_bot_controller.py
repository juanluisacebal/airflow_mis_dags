"""
### DAG: SERVER_bot_controller
 
This DAG orchestrates a Docker-based bot lifecycle in four main steps:
  
1. Prevents overlapping execution by short-circuiting if a recent run is active.
2. Starts a Docker container running a Selenium bot image.
3. Executes a bot script inside the container with a preloaded Chromium profile.
4. Stops the container after execution, regardless of success or failure.
 
Variables used:
- `ruta_bots`: Base path where bot-related files are stored.
- `ruta_volumen_selenium`: Volume path shared with Docker container.
- `min_diff_ejecucion_bot`: Minimum minutes allowed between DAG runs.
 
Tags: ["bot", "docker"]
Schedule: Hourly
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import DagRun, Variable
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.utils.db import provide_session
import subprocess
import os
from datetime import datetime, timedelta
from airflow.utils.timezone import utcnow

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

# Hola

bot_path = Variable.get("ruta_bots")
def build_docker_image():
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log

    build_command = ["docker", "build", "-t", "l-bot-custom", "."]
    logger.info(f"🛠️ Building Docker image with command: {' '.join(build_command)}")
    logger.info(f"[COMMAND] {' '.join(build_command)}")

    try:
        process = subprocess.Popen(build_command, cwd=bot_path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in iter(process.stdout.readline, ''):
            logger.info(line.strip())
        process.stdout.close()
        returncode = process.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, build_command)
        logger.info("✅ Docker image built successfully.")
    except subprocess.CalledProcessError as e:
        logger.warning(f"⚠️ Docker build failed with return code {e.returncode}, but continuing: {e}")
    except Exception as e:
        logger.warning(f"⚠️ Unexpected error during Docker build: {e}, continuing anyway.")

    return True

# Ensure persistent Chromium profile path exists
chromium_profile_path = os.path.join(bot_path, "perfiles_docker", "Juan")
os.makedirs(chromium_profile_path, exist_ok=True)

def run_and_stream(cmd, logger):
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in iter(process.stdout.readline, ''):
        logger.info(line.strip())
    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)

@provide_session
def should_run(session=None, **context):
    """
    Short-circuits the DAG if another run has been active within the last X minutes.
    Uses the Airflow variable 'min_diff_ejecucion_bot' to determine the time window.
    """
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log

    now = utcnow()
    minutes = int(Variable.get("min_diff_ejecucion_bot", default_var=5))
    threshold_time = now - timedelta(minutes=minutes)
    logger.info(f"🕒 Current UTC time: {now.isoformat()}")
    logger.info(f"⏳ Threshold (min_diff_ejecucion_bot): {minutes} minutes ago = {threshold_time.isoformat()}")

    logger.info(f"⏱ Checking for active DAG runs in the past {minutes} minutes (since {threshold_time.isoformat()})...")
    dag_run_id = context["dag_run"].run_id

    from airflow.models import TaskInstance
    dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "SERVER_bot_controller",
            DagRun.execution_date > threshold_time,
        )
        .all()
    )

    recent_runs = []
    for run in dag_runs:
        if run.run_id == dag_run_id:
            continue  # skip current run

        run_bot_state = (
            session.query(TaskInstance.state)
            .filter(
                TaskInstance.dag_id == run.dag_id,
                TaskInstance.run_id == run.run_id,
                TaskInstance.task_id == "run_bot_task",
            )
            .scalar()
        )
        if run_bot_state != State.SKIPPED:
            recent_runs.append(run)

    logger.info(f"🗓 Total recent executions with non-skipped tasks: {len(recent_runs)}")

    if recent_runs:
        most_recent = max([run.execution_date for run in recent_runs])
        minutes_passed = int((now - most_recent).total_seconds() // 60)
        minutes_remaining = minutes - minutes_passed
        logger.info(f"⏱ Time since last run: {minutes_passed} minutes. Remaining: {minutes_remaining} minutes.")

    if recent_runs:
        logger.warning("🚫 A recent active execution exists. Skipping this DAG run.")
        return False

    logger.info("✅ No recent active executions. Proceeding with DAG run.")
    return True

def start_bot():
    """
    Runs the Docker container for the bot in detached mode with the specified volume.
    Logs both stdout and stderr output.
    """
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    volume_path = Variable.get("ruta_volumen_selenium")
    run_command = [
        "docker", "run", "--rm", "-d",
        "--name", "l-bot",
        "-p", "4444:4444", "-p", "5900:5900", "-p", "7900:7900",
        "--shm-size", "2g",
        "-v", f"{volume_path}:/home/seluser/compartido",
        #"seleniarm/standalone-chromium:latest"
        "l-bot-custom"
    ]
    logger.info(f"[COMMAND] {' '.join(run_command)}")
    result = subprocess.run(run_command, capture_output=True, text=True)
    logger.info(f"[docker run] STDOUT:\n{result.stdout}")
    logger.info(f"[docker run] STDERR:\n{result.stderr}")
    result.check_returncode()

def run_bot():
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    """
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log

    # Pre-copy the Chrome profile before executing the script
    profile_src = "/home/seluser/compartido/Perfiles/Profile 1/Default"
    profile_dest = "/home/seluser/.config/chromium/Profile 1/Default"

    copy_command = [
        "docker", "exec", "-u", "seluser", "l-bot",
        "bash", "-c",
        f"mkdir -p \"{profile_dest}\" && cp -r \"{profile_src}/.\" \"{profile_dest}/\""
    ]
    logger.info(f"[COMMAND] {' '.join(copy_command)}")
    run_and_stream(copy_command, logger)

    main_command = [
        "docker", "exec", "-u", "seluser", "-w", "/home/seluser/compartido", "l-bot",
        "python3", "main.py"
    ]
    logger.info(f"[COMMAND] {' '.join(main_command)}")
    run_and_stream(main_command, logger)
    # If main_command fails, run_and_stream already raises an exception
    logger.info("✅ Bot script completed successfully.")

def stop_bot():
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    logger.info(f"[COMMAND] docker stop l-bot")
    result = subprocess.run(["docker", "stop", "l-bot"], capture_output=True, text=True)
    logger.info(f"[docker stop l-bot] STDOUT:\n{result.stdout}")
    logger.info(f"[docker stop l-bot] STDERR:\n{result.stderr}")
    result.check_returncode()

def schedule_reboot():
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    reboot_command = ["sudo", "shutdown", "-r", "+5"]
    logger.info(f"[COMMAND] {' '.join(reboot_command)}")
    result = subprocess.run(reboot_command, capture_output=True, text=True)
    logger.info(f"[reboot command] STDOUT:\n{result.stdout}")
    logger.info(f"[reboot command] STDERR:\n{result.stderr}")

with DAG(
    dag_id="SERVER_bot_controller",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],

    #schedule_interval="30 * * * *",
    #schedule_interval="@hourly",
    catchup=False,
    tags=["bot", "docker"],
    doc_md="""
    ## 📄 DAG Documentation: SERVER_bot_controller
    
    This DAG automates the execution of a Selenium-based bot inside a Docker container.
    
    ### 🔁 Workflow Steps
    - **check_if_should_run**: Prevents overlapping runs by checking if another execution occurred recently.
    - **docker_up**: Spins up a Docker container using the image `l-bot-custom`.
    - **run_bot_task**: Executes `main.py` inside the running container. Prior to execution, it copies a Chromium profile into the container.
    - **docker_down**: Stops the container after bot execution, regardless of success or failure.
    
    ### ⚙️ Configuration Variables
    - `ruta_bots`: Root path of bot project files.
    - `ruta_volumen_selenium`: Host path mounted into Docker container at `/home/seluser/compartido`.
    - `min_diff_ejecucion_bot`: Time threshold (in minutes) to avoid DAG overlap.
    
    ### 🕐 Schedule
    - Every hour (`@hourly`)
    
    ### 🏷 Tags
    - `bot`, `docker`
    """,
) as dag:
    """Hourly DAG that controls a Docker-based bot lifecycle:
    1. Checks recent DAG runs to avoid overlapping execution.
    2. Starts Docker container.
    3. Executes the bot.
    4. Shuts down the Docker container.
    """

    check_if_should_run = ShortCircuitOperator(
        task_id="check_if_should_run",
        python_callable=should_run,
    )
    
    docker_build_image = PythonOperator(
        task_id="docker_build_image",
        python_callable=build_docker_image,
    )

    docker_up = PythonOperator(
        task_id="docker_up",
        python_callable=start_bot,
    )

    run_bot_task = PythonOperator(
        task_id="run_bot_task",
        python_callable=run_bot,
        retries=4,
        retry_delay=timedelta(seconds=15),
    )

    docker_down = PythonOperator(
        task_id="docker_down",
        python_callable=stop_bot,
        trigger_rule="all_done",  # Ensure this runs even if the previous task fails
    )
    
'''    schedule_reboot_task = PythonOperator(
        task_id="schedule_reboot",
        python_callable=schedule_reboot,
    )
'''

check_if_should_run >> docker_build_image >> docker_up >> run_bot_task >> docker_down 
#>> schedule_reboot_task

"""
## 📄 DAG Documentation: SERVER_bot_controller

This DAG automates the execution of a Selenium-based bot inside a Docker container.

### 🔁 Workflow Steps
 - **check_if_should_run**: Prevents overlapping runs by checking if another execution occurred recently.
 - **docker_up**: Spins up a Docker container using the image `l-bot-custom`.
 - **run_bot_task**: Executes `main.py` inside the running container. Prior to execution, it copies a Chromium profile into the container.
 - **docker_down**: Stops the container after bot execution, regardless of success or failure.

### ⚙️ Configuration Variables
 - `ruta_bots`: Root path of bot project files.
 - `ruta_volumen_selenium`: Host path mounted into Docker container at `/home/seluser/compartido`.
 - `min_diff_ejecucion_bot`: Time threshold (in minutes) to avoid DAG overlap.

### 🕐 Schedule
 - Every hour (`@hourly`)

### 🏷 Tags
 - `bot`, `docker`
"""
