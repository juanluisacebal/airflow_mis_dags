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

ruta_airflow = Variable.get("ruta_airflow")
HOSTS = Variable.get("hosts_bot", default_var="s1", deserialize_json=False).split(",")
#HOSTS=['s1','s2','s3']
#HOSTS=['s0-1','s0-2']


MIN=1.1#0.01
MAX=1.5#0.02

bot_path = Variable.get("ruta_bots")

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
            DagRun.dag_id == context["dag"].dag_id,
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
        "--add-host", "host.docker.internal:host-gateway",
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
        "ssh", "-vvv", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-D", str(proxy_port), host_name, "-N", "-f"
    ]
    
    if host_name.startswith("s0"):
        logger.info(f"❌ Skipping SOCKS proxy for {host_name} (starts with 's0').")
    else:
        try:
            logger.info(f"[COMMAND] {' '.join(socks_command)}")
            subprocess.run(socks_command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, check=True)
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



# --- MULTI-DAG GENERATION ---
dags = {}

for host in HOSTS:
    dag_id = f"SERVER_bot_controller_{host}"

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval="@hourly",
        #schedule_interval="@daily",
        #schedule_interval="*/30 * * * *",
        tags=["bot", "docker"],
        doc_md=f"""
        ## 📄 DAG Documentation: {dag_id}

        This DAG automates the execution of a Selenium-based bot inside a Docker container for host `{host}`.

        ### 🔁 Workflow Steps
        - **check_if_should_run**: Prevents overlapping runs.
        - **docker_up**: Spins up a Docker container.
        - **run_bot_task**: Executes `main.py`.
        - **docker_down**: Stops the container.

        ### ⚙️ Configuration Variables
        - `ruta_bots`
        - `min_diff_ejecucion_bot`

        ### 🕐 Schedule
        - Every hour (`@hourly`)
        """,
    ) as dag:
        dags[dag_id] = dag

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

        run_docker_task = PythonOperator(
            task_id=f"{host}_run_docker_t",
            python_callable=run_docker,
            op_kwargs={"host_name": host},
            retries=2,
            trigger_rule="all_done",
            retry_delay=timedelta(seconds=15)
        )

        run_bot_task = PythonOperator(
            task_id=f"{host}_run_bot_t",
            python_callable=run_bot_ssh,
            op_kwargs={"host_name": host},
            retries=2,
            trigger_rule="all_done",
            retry_delay=timedelta(seconds=15)
        )

        docker_down = PythonOperator(
            task_id=f"{host}_docker_down_t",
            python_callable=stop_bot_ssh,
            op_kwargs={"host_name": host},
            retries=1,
            trigger_rule="all_done",
            retry_delay=timedelta(seconds=10)
        )
        if host not in ["s0-1", "s0-2"]:
            docker_prune_and_log_freed_space = BashOperator(
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

        if host  in ["s0-1", "s0-2"]:

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

        check_if_should_run >> docker_build_image >> run_docker_task >> run_bot_task >> docker_down

# Expose all generated DAGs to Airflow
globals().update(dags)
