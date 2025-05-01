"""
### DAG: SERVER_bot_controller
 
This DAG orchestrates a Docker-based bot lifecycle in four main steps:
  
1. Prevents overlapping execution by short-circuiting if a recent run is active.
2. Starts a Docker container running a Selenium bot image.
3. Executes a bot script inside the container with a preloaded Chromium profile.
4. Stops the container after execution, regardless of success or failure.
 
Variables used:
- `ruta_bots`: Base path where bot-related files are stored.
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
import time
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

HOSTS = Variable.get("hosts_bot", default_var="s1", deserialize_json=False).split(",")
MIN=0.01#0.01
MAX=0.5#0.02

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
    print("Finished streaming")
    process.stdout.close()
    returncode = process.wait()
    print(f"Return code: {returncode}")
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
                TaskInstance.task_id == "s0_docker_build_image",
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

def start_bot(container_name="l-bot", port_offset=0):
    logger = LoggingMixin().log
    
    ports = [
        f"{4444+port_offset}:4444",
        f"{5900+port_offset}:5900",
        f"{7900+port_offset}:7900"
    ]
    container_hostname = f"s0-{port_offset}"
    run_command = [
    "docker", "run", "--rm", "-d",
    "--name", container_name,
    "--hostname", container_hostname,  # <-- nuevo parámetro
    "-p", ports[0], "-p", ports[1], "-p", ports[2],
    "--shm-size", "2g",
    "-v", f"{bot_path}:/home/seluser/compartido",
    "l-bot-custom"
    ]
    
    logger.info(f"[COMMAND] {' '.join(run_command)}")
    result = subprocess.run(run_command, capture_output=True, text=True)
    logger.info(f"[docker run] STDOUT:\n{result.stdout}")
    logger.info(f"[docker run] STDERR:\n{result.stderr}")
    result.check_returncode()



def copy_files(container_name="l-bot"):
    time.sleep(5)  # Wait for the container to be fully up and running
    logger = LoggingMixin().log

    profile_src = f"/home/seluser/compartido/Perfiles_s0/Profile 1/Default"
    profile_dest = "/home/seluser/.config/chromium/Profile 1/Default"

    copy_command = [
        "docker", "exec", "-u", "seluser", container_name,
        "bash", "-c",
         f"mkdir -p \"{profile_src}\"",
        f"mkdir -p \"{profile_dest}\" && cp -r \"{profile_src}/.\" \"{profile_dest}/\""
    ]
    logger.info(f"[COMMAND] {' '.join(copy_command)}")
    run_and_stream(copy_command, logger)



def run_bot(container_name="l-bot", config_filename="config.json",**context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    Accepts a config_filename parameter to specify the config file.
    """
    
    from airflow.utils.log.logging_mixin import LoggingMixin
    from datetime import datetime
    import time

    logger = LoggingMixin().log
    ti = context["ti"]

    start_time = utcnow()

    # Esperar si hay una marca de tiempo previa en XCom
    next_time = ti.xcom_pull(task_ids="run_bot_lock", key="next_allowed_time")
    if next_time:
        logger.info(f"⏳ Esperando hasta {next_time} para ejecutar run_bot en s0")
        while utcnow() < datetime.fromisoformat(next_time):
            time.sleep(2)

    # Devolver la próxima hora de inicio permitida
    next_exec = (start_time + timedelta(seconds=60)).isoformat()
    ti.xcom_push(key="next_allowed_time", value=next_exec)
    logger.info(f"✅ Próxima ejecución permitida después de: {next_exec}")

    
    main_command = [
        "docker", "exec", "-t", "-u", "seluser", "-w", "/home/seluser/compartido", container_name,
        "python3", "main.py", f"{MIN}", f"{MAX}", "--config", config_filename
    ]

    logger.info(f"[COMMAND] {' '.join(main_command)}")
    run_and_stream(main_command, logger)
    # If main_command fails, run_and_stream already raises an exception
    logger.info("✅ Bot script completed successfully.")


def stop_bot(container_name="l-bot"):
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
    logger = LoggingMixin().log
    stop_command = ["docker", "stop", container_name]
    logger.info(f"[COMMAND] {' '.join(stop_command)}")
    result = subprocess.run(stop_command, capture_output=True, text=True)
    logger.info(f"[{stop_command}] STDOUT:\n{result.stdout}")
    logger.info(f"[{stop_command}] STDERR:\n{result.stderr}")
    result.check_returncode()


    


def schedule_reboot(host_name):
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    reboot_command = ["ssh", host_name, "sudo reboot"]
    logger.info(f"[COMMAND] {' '.join(reboot_command)}")
    result = subprocess.run(reboot_command, capture_output=True, text=True)
    logger.info(f"[reboot command] STDOUT:\n{result.stdout}")
    logger.info(f"[reboot command] STDERR:\n{result.stderr}")
    time.sleep(20)




def start_bot_ssh(host_name):
    """
    Runs the Docker container for the bot in detached mode with the specified volume.
    Logs both stdout and stderr output.
    """
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    ssh_command = f'ssh {host_name} "docker run --rm -d --name l-bot --network host --hostname {host_name} --dns=8.8.8.8 --ulimit nofile=32768:32768 --shm-size 2g -v {bot_path}:/home/seluser l-bot-custom"'
    logger.info(f"🚀 Running start command: {ssh_command}")
    logger.info(f"[COMMAND] {ssh_command}")
    result = subprocess.run(ssh_command, capture_output=True, text=True, shell=True)
    logger.info(f"[docker run] STDOUT:\n{result.stdout}")
    logger.info(f"[docker run] STDERR:\n{result.stderr}")
    result.check_returncode()

def run_bot_ssh(host_name, **context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    """
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    from airflow.utils.log.logging_mixin import LoggingMixin
    from datetime import datetime
    import time

    logger = LoggingMixin().log
    ti = context["ti"]

    start_time = utcnow()

    # Esperar si hay una marca de tiempo previa en XCom
    next_time = ti.xcom_pull(task_ids="run_bot_lock", key="next_allowed_time")
    if next_time:
        logger.info(f"⏳ Esperando hasta {next_time} para ejecutar {host_name}")
        while utcnow() < datetime.fromisoformat(next_time):
            time.sleep(2)

    # Devolver la próxima hora de inicio permitida
    next_exec = (start_time + timedelta(seconds=60)).isoformat()
    ti.xcom_push(key="next_allowed_time", value=next_exec)
    logger.info(f"✅ Próxima ejecución permitida después de: {next_exec}")

    # Ejecutar el bot
    main_command = f'ssh {host_name} "docker exec -u seluser -w /home/seluser l-bot python3 main.py {MIN} {MAX} --config conf-{host_name}.json --responses resp-{host_name}.json"'
    logger.info(f"[COMMAND] {main_command}")
    run_and_stream_ssh(main_command, logger)
    # If main_command fails, run_and_stream already raises an exception
    logger.info("✅ Bot script completed successfully.")

def stop_bot_ssh(host_name):
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log
    logger.info(f"🚀 Running stop command: ssh {host_name} \"docker stop l-bot\"")
    logger.info(f"[COMMAND] ssh {host_name} \"docker stop l-bot\"")
    result = subprocess.run(f'ssh {host_name} "docker stop l-bot"', capture_output=True, text=True, shell=True)
    logger.info(f"[docker stop l-bot] STDOUT:\n{result.stdout}")
    logger.info(f"[docker stop l-bot] STDERR:\n{result.stderr}")
    result.check_returncode()


    # ⬇️ Copiar archivos JSON del host remoto a local
    local_path = os.path.join(bot_path, "json-remotos")
    os.makedirs(local_path, exist_ok=True)
    remote_path = os.path.join(bot_path, "*.json")
    copy_command = f"scp -v {host_name}:{remote_path} {local_path}/"
    logger.info(f"[COMMAND] {copy_command}")
    result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
    logger.info(f"[scp JSON files] STDOUT:\n{result.stdout}")
    logger.info(f"[scp JSON files] STDERR:\n{result.stderr}")



def build_docker_image_ssh(host_name):
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    from airflow.utils.log.logging_mixin import LoggingMixin
    logger = LoggingMixin().log

    # Mostrar el PATH actual para debug
    print(f"🧪 PATH actual (incluye /snap/bin): {os.environ['PATH']}")
    result = subprocess.run("which gcloud", shell=True, capture_output=True, text=True)
    logger.info(f"🧪 which gcloud → {result.stdout.strip()}")

    # 1) Copiar archivos
    sync_cmd = f'scp {bot_path}/main.py  {host_name}:{bot_path}/'
    # {bot_path}/Dockerfile
    logger.info(f"[COMMAND] {sync_cmd}")
    subprocess.run(sync_cmd, shell=True, check=True, text=True)

    # 2) Build en remoto

    build_command = f'bash {bot_path}/build_remote.sh {host_name}'
    #"cd {bot_path} && docker build --network=host -t l-bot-custom ."'    
    logger.info(f"🛠️ Building Docker image with command: {build_command}")
    logger.info(f"[COMMAND] {build_command}")

    try:
        process = subprocess.Popen(build_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=True)
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

def run_and_stream_ssh(cmd, logger):
    os.environ["PATH"] += os.pathsep + "/snap/bin"
    logger.info(f"🚀 Running command: {cmd}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=isinstance(cmd, str))
    for line in iter(process.stdout.readline, ''):
        logger.info(line.strip())
    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)


def nada():
    print("Nada")


with DAG(
    dag_id="SERVER_bot_controller",
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],

    #schedule_interval="30 * * * *",
    #schedule_interval="@hourly",
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

    docker_up_1 = PythonOperator(
        task_id="s0-1_docker_up_t",
        python_callable=nada, #start_bot,
        op_kwargs={"container_name": "l-bot-1", "port_offset": 0},
    )

    docker_up_2 = PythonOperator(
        task_id="s0-2_docker_up_t",
        python_callable=nada, #start_bot,
        op_kwargs={"container_name": "l-bot-2", "port_offset": 1},
    )

    copy_files_task_1 = PythonOperator(
        task_id="s0-1_copy_files_t",
        python_callable=copy_files,
        op_kwargs={"container_name": "l-bot-1"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    copy_files_task_2 = PythonOperator(
        task_id="s0-2_copy_files_t",
        python_callable=copy_files,
        op_kwargs={"container_name": "l-bot-2"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    run_bot_task_1 = PythonOperator(
        task_id="s0-1_run_bot_t",
        python_callable=run_bot,
        op_kwargs={"container_name": "l-bot-1", "config_filename": "config.json"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    run_bot_task_2 = PythonOperator(
        task_id="s0-2_run_bot_t",
        python_callable=run_bot, #nada,
        op_kwargs={"container_name": "l-bot-2", "config_filename": "config-u2.json"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    docker_down_1 = PythonOperator(
        task_id="s0-1_docker_down_t",
        python_callable=stop_bot,
        op_kwargs={"container_name": "l-bot-1"},
        trigger_rule="all_done",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    docker_down_2 = PythonOperator(
        task_id="s0-2_docker_down_t",
        python_callable=stop_bot,
        op_kwargs={"container_name": "l-bot-2"},
        trigger_rule="all_done",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )


    check_if_should_run = ShortCircuitOperator(
        task_id="check_should_run",
        python_callable=should_run,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    
    docker_build_image = PythonOperator(
        task_id="s0_docker_build_t",
        python_callable=build_docker_image,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )



    call_llm_task = PythonOperator(
        task_id="s0_call_llm_t",
        python_callable=nada,
        retries=4,
        retry_delay=timedelta(seconds=15),
    )

    s0_prune_log_t = BashOperator(
        task_id=f"s0_prune_log_t",
        bash_command=f'''
        echo "📦 Docker usage before:"
        BEFORE=$("docker system df -v" | tee /tmp/docker_before.txt | grep "Total space used" | awk '{{print $4, $5}}')
        "docker system prune -a --volumes --force"
        echo "📦 Docker usage after:"
        AFTER=$("docker system df -v" | tee /tmp/docker_after.txt | grep "Total space used" | awk '{{print $4, $5}}')
        echo "🧹 Freed space: $BEFORE -> $AFTER"
        ''',
        retries=3,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done",
    )





dynamic_tasks = {}

for i, host in enumerate(HOSTS):
    prev_host = HOSTS[i - 1] if i > 0 else None

    dynamic_tasks[f"{host}_docker_build_image"] = PythonOperator(
        task_id=f"{host}_docker_build_t",
        python_callable=build_docker_image_ssh,
        op_kwargs={"host_name": host},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    dynamic_tasks[f"{host}_docker_up"] = PythonOperator(
        task_id=f"{host}_docker_up_t",
        python_callable=start_bot_ssh,
        op_kwargs={"host_name": host},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    dynamic_tasks[f"{host}_run_bot_task"] = PythonOperator(
        task_id=f"{host}_run_bot_t",
        python_callable=run_bot_ssh,
        op_kwargs={"host_name": host},
        retries=1,
        retry_delay=timedelta(seconds=10),
    )
    if prev_host:
        dynamic_tasks[f"{host}_run_bot_task"].op_kwargs["prev_task_id"] = f"{prev_host}_run_bot_t"
    
    dynamic_tasks[f"{host}_docker_down"] = PythonOperator(
        task_id=f"{host}_docker_down_t",
        python_callable=stop_bot_ssh,
        op_kwargs={"host_name": host},
        retries=1,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10),
    )

    dynamic_tasks[f"{host}_reboot"] = PythonOperator(
        task_id=f"{host}_reboot_t",
        python_callable=schedule_reboot,
        op_kwargs={"host_name": host},
        retries=1,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done",
    )
    dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"] = BashOperator(
        task_id=f"{host}_prune_log_t",
        bash_command=f'''
        echo "📦 Docker usage before:"
        BEFORE=$(ssh {host} "docker system df -v" | tee /tmp/docker_before.txt | grep "Total space used" | awk '{{print $4, $5}}')
        ssh {host}  "docker system prune -a --volumes --force"
        ssh {host} 'rm -r /home/juanlu/Documentos/BOTS/google-chrome/Profile\ 1_*'
        echo "📦 Docker usage after:"
        AFTER=$(ssh {host} "docker system df -v" | tee /tmp/docker_after.txt | grep "Total space used" | awk '{{print $4, $5}}')
        echo "🧹 Freed space: $BEFORE -> $AFTER"
        ''',
        retries=3,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done",
    )


check_if_should_run >> call_llm_task >> docker_build_image >>[ docker_up_1, docker_up_2]
docker_up_1 >> copy_files_task_1 >> run_bot_task_1 >> docker_down_1
docker_up_2 >> copy_files_task_2 >> run_bot_task_2 >> docker_down_2 
[docker_down_1, docker_down_2] >> s0_prune_log_t

for host in HOSTS:
    check_if_should_run >> dynamic_tasks[f"{host}_docker_build_image"]
    dynamic_tasks[f"{host}_docker_build_image"] >> dynamic_tasks[f"{host}_docker_up"]
    dynamic_tasks[f"{host}_docker_up"] >> dynamic_tasks[f"{host}_run_bot_task"]
    dynamic_tasks[f"{host}_run_bot_task"] >> dynamic_tasks[f"{host}_docker_down"]
    dynamic_tasks[f"{host}_docker_down"] >> dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"]
    dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"] >> dynamic_tasks[f"{host}_reboot"]
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
 - `min_diff_ejecucion_bot`: Time threshold (in minutes) to avoid DAG overlap.

### 🕐 Schedule
 - Every hour (`@hourly`)

### 🏷 Tags
 - `bot`, `docker`
"""
