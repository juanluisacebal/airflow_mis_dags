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
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import TaskInstance
from datetime import datetime
import time

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

ruta_airflow = Variable.get("ruta_airflow")
HOSTS = Variable.get("hosts_bot", default_var="s1", deserialize_json=False).split(",")


MIN=0.25#0.01
MAX=0.3#0.02

bot_path = Variable.get("ruta_bots")
def build_docker_image():
    logger = LoggingMixin().log

    build_command = ["docker", "build", "-t", "l-bot-custom", "."]
    logger.info(f"🛠️ Building Docker image with command: {' '.join(build_command)}")
    logger.info(f"[COMMAND] {' '.join(build_command)}")

    try:
        # Set timeout to 30 minutes
        process = subprocess.run(
            build_command,
            cwd=bot_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=1800,  # 30 minutes
            check=True
        )
        
        # Log the output line by line
        for line in process.stdout.splitlines():
            logger.info(line)
            
        logger.info("✅ Docker image built successfully.")
        
    except subprocess.TimeoutExpired as e:
        logger.error(f"⚠️ Docker build timed out after 30 minutes: {e}")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"⚠️ Docker build failed with return code {e.returncode}")
        logger.error("Build output:")
        if e.output:
            for line in e.output.splitlines():
                logger.error(line)
        raise
    except Exception as e:
        logger.error(f"⚠️ Unexpected error during Docker build: {str(e)}")
        raise

    return True

# Ensure persistent Chromium profile path exists
#chromium_profile_path = os.path.join(bot_path, "perfiles_docker", "Juan")
#os.makedirs(chromium_profile_path, exist_ok=True)

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
    logger = LoggingMixin().log

    now = utcnow()
    minutes = int(Variable.get("min_diff_ejecucion_bot", default_var=5))
    threshold_time = now - timedelta(minutes=minutes)
    logger.info(f"🕒 Current UTC time: {now.isoformat()}")
    logger.info(f"⏳ Threshold (min_diff_ejecucion_bot): {minutes} minutes ago = {threshold_time.isoformat()}")
    logger.info(f"⏱ Checking for active DAG runs in the past {minutes} minutes (since {threshold_time.isoformat()})...")
    dag_run_id = context["dag_run"].run_id
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
    #"--ulimit", "nofile=32768",
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



def run_bot(container_name="l-bot", config_filename="config.json", responses_filename='json-remotos/resp-s0.json',**context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    Accepts a config_filename parameter to specify the config file.
    """
    
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
        "python3", "main.py", "--config", config_filename, "--responses", responses_filename, f"{MIN}", f"{MAX}"
    ]

    logger.info(f"[COMMAND] {' '.join(main_command)}")
    #logger.info(f"⏳ Esperando 1200 segundos")
    #time.sleep(1200)
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
    logger = LoggingMixin().log

    ports = [
        "4444:4444",
        "5900:5900",
        "7900:7900"
    ]
    imagen = "l-bot-custom"
    container_hostname = f"{host_name}"
    run_command = [
        "ssh", host_name,
        f"docker", "run", "--rm", "-d",
        "--name","l-bot",
        "--hostname", f"{container_hostname} ",
        f"--network", "host", 
        "--dns=8.8.8.8",
        "--ulimit", "nofile=32768",
        f"-p {ports[0]} -p {ports[1]} -p {ports[2]} ",
        "--shm-size 2g",
        #"-e SE_VNC_NO_PASSWORD=false",
        #"-e SE_VNC_PORT=5900",
        #"-e SE_START_VNC=true",
        "-v", "tmpfs:/tmp",
        "-v", "tmpfs:/home/seluser/.cache",
        f"-v {bot_path}/{host_name}:/home/seluser/compartido {imagen}"
    ]

    logger.info(f"[COMMAND] {' '.join(run_command)}")
    result = subprocess.run(run_command, capture_output=True, text=True)
    logger.info(f"[docker run] STDOUT:\n{result.stdout}")
    logger.info(f"[docker run] STDERR:\n{result.stderr}")
    result.check_returncode()

    # Instalar selenium y webdriver-manager dentro del contenedor ya iniciado
    install_cmd = f'ssh {host_name} "docker exec -u seluser l-bot pip install selenium==4.19.0 webdriver-manager==4.0.1"'
    logger.info(f"[COMMAND] {install_cmd}")
    #result = subprocess.run(install_cmd, capture_output=True, text=True, shell=True)
    #logger.info(f"[pip install] STDOUT:\n{result.stdout}")
    #logger.info(f"[pip install] STDERR:\n{result.stderr}")
    result.check_returncode()

    #

def run_bot_ssh(host_name, **context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    """
    logger = LoggingMixin().log
    # BORRAR:
    
    #cmd_cp = f' ssh {host_name} "sudo cp -a /home/seluser/.config/google-chrome /home/seluser/"'
    #logger.info(f"[cp] {cmd_cp}")
    #result = subprocess.run(cmd_cp, capture_output=True, text=True, shell=True)
    #logger.info(f"[cp] STDOUT:\n{result.stdout}")
    #logger.info(f"[cp] STDERR:\n{result.stderr}")

    if host_name == "s3" or host_name == "s2" or host_name == "s1":
        #BORRAR cmd_cp = f' ssh {host_name} "sudo cp -a /home/seluser/.config/google-chrome /home/seluser/"'  # BORRAR
        cmd = f'ssh {host_name} "docker exec -d l-bot x11vnc -display :99.0 -nopw -forever -logfile /tmp/x11vnc.log"'
        logger.info(f"[COMMAND] {cmd}")
        #result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        #logger.info(f"[x11vnc] STDOUT:\n{result.stdout}")
        #logger.info(f"[x11vnc] STDERR:\n{result.stderr}")


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
    #main_command = f'ssh {host_name} "docker exec -u seluser -w /home/seluser/compartido l-bot python3 main.py --config conf-{host_name}.json --responses resp-{host_name}.json {MIN} {MAX} "'
    #logger.info(f"[COMMAND] {main_command}")
    
    # Para s1, esperar 1200 segundos antes de ejecutar el comando para usar el contenedor y comprobaciones
    #if host_name == "s1":
    #logger.info(f"⏳ Esperando 1200 segundos")
    #time.sleep(1200)

    # run_and_stream_ssh(main_command, logger)
    # If main_command fails, run_and_stream already raises an exception
    #result = subprocess.run(main_command, shell=True, capture_output=True, text=True)
    #logger.info(f"[stdout]\n{result.stdout}")
    #logger.info(f"[stderr]\n{result.stderr}")
    #result.check_returncode()

    #process = subprocess.Popen(main_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    #for line in iter(process.stdout.readline, ''):
    #    logger.info(line.strip())
    #process.stdout.close()
    #returncode = process.wait()
    #if returncode != 0:
    #    raise subprocess.CalledProcessError(returncode, main_command)
    
    main_command = [
    "ssh", host_name,
    "docker", "exec", "-u", "seluser", "-w", f"/home/seluser/compartido", "l-bot",
    "python3", "main.py",
    "--config", f"conf-{host_name}.json",
    "--responses", f"resp-{host_name}.json",
    str(MIN), str(MAX)
    ]
    process = subprocess.Popen(main_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    for line in iter(process.stdout.readline, ''):
        if line:
            logger.info(line.strip())

    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, main_command)

    logger.info("✅ Bot script completed successfully.")    
    logger.info("✅ Bot script completed successfully.")



def stop_bot_ssh(host_name):
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
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
    remote_path = os.path.join(bot_path, host_name, "*.json")
    copy_command = f"scp -v {host_name}:{remote_path} {local_path}/"
    logger.info(f"[COMMAND] {copy_command}")
    result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
    logger.info(f"[scp JSON files] STDOUT:\n{result.stdout}")
    logger.info(f"[scp JSON files] STDERR:\n{result.stderr}")

    local_path = os.path.join(bot_path, "debug-logs")
    remote_path = os.path.join(bot_path, host_name, "debug-logs")
    patterns = ["log", "png", "html"]
    remote_sources = " ".join(f"{host_name}:{remote_path}/*.{ext}" for ext in patterns)
    copy_command = f"scp -v {remote_sources} {local_path}/"
    logger.info(f"[COMMAND] {copy_command}")
    result = subprocess.run(copy_command, shell=True, capture_output=True, text=True)
    logger.info(f"[scp logs files] STDOUT:\n{result.stdout}")
    logger.info(f"[scp logs files] STDERR:\n{result.stderr}")



def build_docker_image_ssh(host_name):
    logger = LoggingMixin().log
    logger.info(f"⬆️ Copiar archivo main.py del local al remoto")
    # 1) Copiar archivos
    sync_cmd = f'scp  -v {bot_path}/main.py  {bot_path}/{host_name}/Dockerfile {host_name}:{bot_path}/{host_name}/'
    # {bot_path}/Dockerfile
    logger.info(f"[COMMAND] {sync_cmd}")
    subprocess.run(sync_cmd, shell=True, check=True, text=True)

    logger.info(f"⬆️ Copiar archivos JSON específicos del local al remoto")
    # ⬆️ Copiar archivos JSON específicos del local al remoto
    local_conf = os.path.join(bot_path, "json-remotos", f"conf-{host_name}.json")
    local_resp = os.path.join(bot_path, "json-remotos",  f"resp-{host_name}.json")
    remote_dest = f"{host_name}:{bot_path}/{host_name}/"

    for local_file in [local_conf, local_resp]:
        copy_json_cmd = f"scp -v {local_file} {remote_dest}"
        logger.info(f"[COMMAND] {copy_json_cmd}")
        result = subprocess.run(copy_json_cmd, shell=True, capture_output=True, text=True)
        logger.info(f"[scp -v {os.path.basename(local_file)}] STDOUT:\n{result.stdout}")
        logger.info(f"[scp -v {os.path.basename(local_file)}] STDERR:\n{result.stderr}")

    # 2) Build en remoto via SSH
    build_cmd = [
        "ssh", host_name,
        "bash", "-lc",
        f"cd {bot_path}/{host_name} && pwd && ls -la && "
        f"docker build --network=host -t l-bot-custom "
        f"-f {bot_path}/{host_name}/Dockerfile {bot_path}/{host_name}"
    ]
    logger.info(f"🛠️ Building via SSH: {' '.join(build_cmd)}")

    # Ejecutar SIN check=True para que no lance la excepción de inmediato
    result = subprocess.run(
        build_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=600
    )

    # Loggear TODO lo que devolvió Docker
    for line in result.stdout.splitlines():
        logger.info(line)

    # Si falló, que se vea bien
    if result.returncode != 0:
        logger.error(f"🚨 Remote build exited {result.returncode}")
        # relanzar incluyendo el output para que Airflow marque FAIL
        raise subprocess.CalledProcessError(
            result.returncode, build_cmd, output=result.stdout
        )

    logger.info("✅ Docker image built successfully on remote host.")

def run_and_stream_ssh(cmd, logger):
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
    #schedule_interval=default_args["schedule_interval"],

    schedule_interval="00,15,30,45 * * * *",
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
        python_callable=start_bot,
        op_kwargs={"container_name": "l-bot-1", "port_offset": 0},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    docker_up_2 = PythonOperator(
        task_id="s0-2_docker_up_t",
        python_callable=start_bot,
        op_kwargs={"container_name": "l-bot-2", "port_offset": 1},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    copy_files_task_1 = PythonOperator(
        task_id="s0-1_copy_files_t",
        python_callable=copy_files,
        op_kwargs={"container_name": "l-bot-1"},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    copy_files_task_2 = PythonOperator(
        task_id="s0-2_copy_files_t",
        python_callable=copy_files,
        op_kwargs={"container_name": "l-bot-2"},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    run_bot_task_1 = PythonOperator(
        task_id="s0-1_run_bot_t",
        python_callable=run_bot,
        op_kwargs={"container_name": "l-bot-1", "config_filename": "json-remotos/conf-s0-0.json"},
        retries=7,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=15)
    )

    run_bot_task_2 = PythonOperator(
        task_id="s0-2_run_bot_t",
        python_callable=run_bot, #nada,
        op_kwargs={"container_name": "l-bot-2", "config_filename": "json-remotos/conf-s0-1.json"},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    docker_down_1 = PythonOperator(
        task_id="s0-1_docker_down_t",
        python_callable=stop_bot,
        op_kwargs={"container_name": "l-bot-1"},
        trigger_rule="all_done",
        retries=2,
        retry_delay=timedelta(seconds=10)
    )

    docker_down_2 = PythonOperator(
        task_id="s0-2_docker_down_t",
        python_callable=stop_bot,
        op_kwargs={"container_name": "l-bot-2"},
        trigger_rule="all_done",
        retries=2,
        retry_delay=timedelta(seconds=10)
    )


    check_if_should_run = ShortCircuitOperator(
        task_id="check_should_run",
        python_callable=should_run,
        retries=2,
        retry_delay=timedelta(seconds=10)
    )
    
    docker_build_image = PythonOperator(
        task_id="s0_docker_build_t",
        python_callable=build_docker_image,
        retries=2,
        retry_delay=timedelta(seconds=10)
    )



    call_llm_task = BashOperator(
        task_id="ALL_call_llm_t",
        bash_command=f"""python3 {ruta_airflow}/scripts/llm.py
        sleep 40                
        """,
        retries=2,
        retry_delay=timedelta(seconds=10)
    )

    s0_prune_log_t = BashOperator(
        task_id=f"s0_prune_log_t",
        bash_command=f'''
        echo "📦 Docker usage before:"
        BEFORE=$("docker system df -v" | tee /tmp/docker_before.txt | grep "Total space used" | awk '{{print $4, $5}}')
        docker system prune -a --volumes --force
        echo "📦 Docker usage after:"
        AFTER=$("docker system df -v" | tee /tmp/docker_after.txt | grep "Total space used" | awk '{{print $4, $5}}')
        echo "🧹 Freed space: $BEFORE -> $AFTER"
        ''',
        retries=2,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done"
    )





dynamic_tasks = {}

for i, host in enumerate(HOSTS):
    prev_host = HOSTS[i - 1] if i > 0 else None

    dynamic_tasks[f"{host}_docker_build_image"] = PythonOperator(
        task_id=f"{host}_docker_build_t",
        python_callable=build_docker_image_ssh,
        op_kwargs={"host_name": host},
        retries=5,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=15)
    )

    dynamic_tasks[f"{host}_docker_up"] = PythonOperator(
        task_id=f"{host}_docker_up_t",
        python_callable=start_bot_ssh,
        op_kwargs={"host_name": host},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    dynamic_tasks[f"{host}_run_bot_task"] = PythonOperator(
        task_id=f"{host}_run_bot_t",
        python_callable=run_bot_ssh,
        op_kwargs={"host_name": host},
        retries=7,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=15)
    )
    if prev_host:
        dynamic_tasks[f"{host}_run_bot_task"].op_kwargs["prev_task_id"] = f"{prev_host}_run_bot_t"
    
    dynamic_tasks[f"{host}_docker_down"] = PythonOperator(
        task_id=f"{host}_docker_down_t",
        python_callable=stop_bot_ssh,
        op_kwargs={"host_name": host},
        retries=1,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=10)
    )

    dynamic_tasks[f"{host}_reboot"] = PythonOperator(
        task_id=f"{host}_reboot_t",
        python_callable=schedule_reboot,
        op_kwargs={"host_name": host},
        retries=1,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done"
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
        retries=2,
        retry_delay=timedelta(seconds=10),
        trigger_rule="all_done"
    )
#        ssh {host}  "docker system prune -a --volumes --force"


check_if_should_run  >> docker_build_image >>[ docker_up_1, docker_up_2]
docker_up_1 >> copy_files_task_1 >> run_bot_task_1 >> docker_down_1
docker_up_2 >> copy_files_task_2 >> run_bot_task_2 >> docker_down_2 
[docker_down_1, docker_down_2] >> s0_prune_log_t

for host in HOSTS:
    [check_if_should_run  ] >> dynamic_tasks[f"{host}_docker_build_image"]
    dynamic_tasks[f"{host}_docker_build_image"] >> dynamic_tasks[f"{host}_docker_up"]
    dynamic_tasks[f"{host}_docker_up"] >> dynamic_tasks[f"{host}_run_bot_task"]
    dynamic_tasks[f"{host}_run_bot_task"] >> dynamic_tasks[f"{host}_docker_down"]
    [dynamic_tasks[f"{host}_docker_down"] ]>> dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"]
    [dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"], s0_prune_log_t ]>> dynamic_tasks[f"{host}_reboot"]
    [dynamic_tasks[f"{host}_reboot"]  ]>> call_llm_task
    #[docker_down_1, docker_down_2] >> s0_prune_log_t
    #    [dynamic_tasks[f"{host}_docker_down"] , docker_down_1, docker_down_2]>> [dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"],s0_prune_log_t]


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
