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
import psutil  # al inicio del archivo si no lo tienes



default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

minutes = int(Variable.get("min_diff_ejecucion_bot", default_var=5))


ruta_airflow = Variable.get("ruta_airflow")
HOSTS = Variable.get("hosts_bot", default_var="s1", deserialize_json=False).split(",")
#HOSTS=['s1','s2','s3']
HOSTS= ['s4']
#HOSTS=['s0-1','s0-2']
minutes = 1200000

MIN=0.13#0.01
MAX=0.15#0.02

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


@provide_session
def should_run(session=None, **context):
    """
    Short-circuits the DAG if another run has been active within the last X minutes.
    Uses the Airflow variable 'min_diff_ejecucion_bot' to determine the time window.
    """
    logger = LoggingMixin().log

    now = utcnow()
    threshold_time = now - timedelta(minutes=minutes)
    logger.info(f"🕒 Current UTC time: {now.isoformat()}")
    logger.info(f"⏳ Threshold (min_diff_ejecucion_bot): {minutes} minutes ago = {threshold_time.isoformat()}")
    logger.info(f"⏱ Checking for active DAG runs in the past {minutes} minutes (since {threshold_time.isoformat()})...")
    dag_run_id = context["dag_run"].run_id
    dag_runs = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "SERVER_bot_controller_ssh",
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
                TaskInstance.task_id == "s0_docker_build_t",
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
    run_and_stream_ssh(copy_command, logger)





def schedule_reboot(host_name):
    logger = LoggingMixin().log
    reboot_command = ["ssh", "-vvv", host_name, "sudo reboot"]
    logger.info(f"[COMMAND] {' '.join(reboot_command)}")
    if host_name.startswith("s0"):
        logger.info(f"❌ Skipping reboot for {host_name} (starts with 's0').")
        return
    logger.info(f"🚀 Rebooting {host_name}...")
    result = subprocess.run(reboot_command, capture_output=True, text=True)
    logger.info(f"[reboot command] STDOUT:\n{result.stdout}")
    logger.info(f"[reboot command] STDERR:\n{result.stderr}")
    time.sleep(20)


def kill_process_on_port(port):
    for conn in psutil.net_connections(kind="inet"):
        if conn.status == psutil.CONN_LISTEN and conn.laddr.port == port:
            if conn.pid:
                try:
                    os.kill(conn.pid, 9)
                    print(f"🧹 Killed process {conn.pid} on port {port}")
                except Exception as e:
                    print(f"❌ Could not kill PID {conn.pid}: {e}")


def run_docker(host_name, **context):
    logger = LoggingMixin().log
    # Cleanup /tmp/{hostname}*/ immediately before running the container
    cleanup_command = f'rm -rf /tmp/{host_name}*/*'
    logger.info(f"[CLEANUP] Running: {cleanup_command}")
    subprocess.run(cleanup_command, shell=True, check=False)

    
    
    port_offset = int(host_name[-1])  # extrae 1 de 's1'
    ports = [
        f"{4445 + port_offset}:4444",
        f"{5901 + port_offset}:5900",
        f"{7901 + port_offset}:7900"
    ]
    if host_name == "s0-1" or host_name == "s0-2":
        ports = [
        f"{4443 + port_offset}:4444",
        f"{5899 + port_offset}:5900",
        f"{7899 + port_offset}:7900"
    ]


    run_command = [
        "docker", "run", "--rm",
        "-d",
        "--name", f"l-bot-{host_name}",
        "--network", "bridge",
        "--add-host=host.docker.internal:172.17.0.1",
        #"--ulimit", "nofile=32768",
        "--hostname", host_name,
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





def run_bot_ssh(host_name, **context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    """
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
    next_exec = (start_time + timedelta(seconds=120)).isoformat()
    ti.xcom_push(key="next_allowed_time", value=next_exec)
    logger.info(f"✅ Próxima ejecución permitida después de: {next_exec}")

    # Crear túnel SOCKS5 en el host
    proxy_base_port = 1080
    proxy_offset = int(host_name[-1]) if host_name[-1].isdigit() else 0
    proxy_port = proxy_base_port + proxy_offset

    if not host_name.startswith("s0"):
        logger.info(f"❌ Killing SOCKS proxy for {host_name} on port {proxy_port}...")
        kill_process_on_port(proxy_port)
        subprocess.run(f"pkill -f 'ssh.*-D {proxy_port}'", shell=True)
    else:
        logger.info(f"❌ Skipping SOCKS kill proxy for {host_name}.")
        
    socks_command = [
        "ssh", #"-v", 
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        f"-D", f"0.0.0.0:{proxy_port}", host_name, "-N", "-f"
    ]
    
    if host_name.startswith("s0"):
        logger.info(f"❌ Skipping SOCKS proxy for {host_name} (starts with 's0').")
    else:
        try:
            logger.info(f"[COMMAND] {' '.join(socks_command)}")
            #subprocess.run(socks_command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, check=True)
            
            process = subprocess.Popen(
            socks_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
            for _ in range(20):  # Solo las 10 primeras líneas
                line = process.stdout.readline()
                if not line:
                    break
                logger.info(f"[ssh -D] {line.strip()}")

            process.wait()
            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, socks_command)

            logger.info(f"✅ SOCKS proxy launched on port {proxy_port}")
        except Exception as e:
            logger.error(f"❌ Error launching SOCKS proxy: {e}")
            raise

    tiempo_minimo=MIN
    tiempo_maximo=MAX

    main_command = [
        "docker", "exec", "-u", "seluser", "-w", f"/home/seluser/compartido", f"l-bot-{host_name}",
        "python3", "main.py",
        "--config", f"json/conf-{host_name}.json",
        "--responses", f"json/resp-s0.json" if host_name.startswith("s0") else f"json/resp-{host_name}.json",
        str(tiempo_minimo), str(tiempo_maximo)
    ]

    logger.info(f"[COMMAND] {' '.join(main_command)}")

    process = subprocess.Popen(main_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    for line in iter(process.stdout.readline, ''):
        if line:
            logger.info(line.strip())

    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, main_command)

    logger.info("✅ Bot script completed successfully.")


    if not host_name.startswith("s0"):
        logger.info(f"❌ Killing SOCKS proxy for {host_name} on port {proxy_port}...")
        kill_process_on_port(proxy_port)
        subprocess.run(f"pkill -f 'ssh.*-D {proxy_port}'", shell=True)
    else:
        logger.info(f"❌ Skipping SOCKS kill proxy for {host_name}.")
        

def stop_bot_ssh(host_name):
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
    logger = LoggingMixin().log
    logger.info(f"🚀 Running stop command: docker stop l-bot-{host_name}")
    logger.info(f"[COMMAND] docker stop l-bot-{host_name}")
    result = subprocess.run(["docker", "stop", f"l-bot-{host_name}"], capture_output=True, text=True)    
    logger.info(f"[docker stop l-bot] STDOUT:\n{result.stdout}")
    logger.info(f"[docker stop l-bot] STDERR:\n{result.stderr}")
    result.check_returncode()



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
    dag_id="SERVER_bot_controller_ssh",
    default_args=default_args,
    #schedule_interval=default_args["schedule_interval"],
    #schedule_interval="*/5 * * * *",
    #schedule_interval="00,15,30,45 * * * *",
    schedule_interval="@hourly",
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
        docker stop l-bot-1 l-bot-2 l-bot-s3 l-bot-s2 l-bot-s1
        sleep 5                
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

# DAG construction: Only create reboot tasks for hosts not starting with "s0"
for i, host in enumerate(HOSTS):
    prev_host = HOSTS[i - 1] if i > 0 else None

    dynamic_tasks[f"{host}_run_docker_task"] = PythonOperator(
        task_id=f"{host}_run_docker_t",
        python_callable=run_docker,
        op_kwargs={"host_name": host},
        retries=2,
        trigger_rule="all_done",
        retry_delay=timedelta(seconds=15)
    )

    dynamic_tasks[f"{host}_run_bot_task"] = PythonOperator(
        task_id=f"{host}_run_bot_t",
        python_callable=run_bot_ssh,
        op_kwargs={"host_name": host},
        retries=2,
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

    # Only create reboot task for hosts not starting with "s0"
    if not host.startswith("s0"):
        dynamic_tasks[f"{host}_reboot"] = PythonOperator(
            task_id=f"{host}_reboot_t",
            python_callable=schedule_reboot,
            op_kwargs={"host_name": host},
            retries=1,
            retry_delay=timedelta(seconds=10),
            trigger_rule="all_done"
        )
# Only create prune task for hosts not in ["s0-1", "s0-2"]
    if host not in ["s0-1", "s0-2"]:
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


check_if_should_run  >> docker_build_image 
s0_prune_log_t >> call_llm_task



for host in HOSTS:
    [docker_build_image] >> dynamic_tasks[f"{host}_run_docker_task"]
    dynamic_tasks[f"{host}_run_docker_task"] >> dynamic_tasks[f"{host}_run_bot_task"]
    dynamic_tasks[f"{host}_run_bot_task"] >> dynamic_tasks[f"{host}_docker_down"]
    # Handle prune and dependencies
    if host not in ["s0-1", "s0-2"]:
        [dynamic_tasks[f"{host}_docker_down"]] >> dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"]
        dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"] >> s0_prune_log_t
    else:
        dynamic_tasks[f"{host}_docker_down"] >> s0_prune_log_t
    # Ensure prune -> reboot -> s0_prune_log_t sequence for hosts not starting with "s0"
    if not host.startswith("s0"):
        dynamic_tasks[f"{host}_docker_prune_and_log_freed_space"] >> dynamic_tasks[f"{host}_reboot"]
        dynamic_tasks[f"{host}_reboot"] >> s0_prune_log_t

# s0_prune_log_t leads to call_llm_task
s0_prune_log_t >> call_llm_task


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
