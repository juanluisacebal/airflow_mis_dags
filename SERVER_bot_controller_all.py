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
from airflow.models import TaskInstance
from datetime import datetime
import time
import psutil  # al inicio del archivo si no lo tienes
import requests
from airflow.models import Variable
from airflow.models import Variable
from airflow.utils.timezone import utcnow
import logging
import json
import requests
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from airflow.sensors.python import PythonSensor



default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

ruta_airflow = Variable.get("ruta_airflow")
HOSTS = Variable.get("hosts_bot", default_var="s1", deserialize_json=False).split(",")
#HOSTS=['s1','s2','s3']
#HOSTS=['s0-1','s0-2']

MINUTES_MIN = 110
MINUTES_MAX = 120

MIN = MINUTES_MIN / 60
MAX = MINUTES_MAX / 60


bot_path = Variable.get("ruta_bots")



def call_gemini_llm(host, **context):
    HOST = host[:2]
    logger = LoggingMixin().log
    api_key = Variable.get("gemini")
    url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}

    input_path = f"{bot_path}/json/resp-{HOST}.json"
    output_path = f"{bot_path}/resp-prov-{HOST}.json"

    with open(input_path, "r") as f:
        data = json.load(f)

    preguntas = data.get("preguntas", {})
    respuestas = {}

    # Preparar contexto con preguntas ya respondidas
    contexto_respondidas = [
        f"Q: {q}\nA: {v['respuesta']}"
        for q, v in preguntas.items()
        if v.get("respuesta")
    ]
    contexto_texto = "\n\n".join(contexto_respondidas)

    # Limitar a 3 preguntas sin respuesta
    preguntas_pendientes = [
        (pregunta, detalle) for pregunta, detalle in preguntas.items() if not detalle.get("respuesta")
    ][:3]

    for pregunta, detalle in preguntas_pendientes:
        tipo = detalle.get("tipo", "")
        opciones = detalle.get("opciones", [])
        opciones_text = "\n".join(opciones) if opciones else ""
        prompt_principal = (
            f"""Contesta brevemente la siguiente pregunta considerando que es de tipo '{tipo}':\nPregunta: {pregunta}\nOpciones:\n{opciones_text}"""
            if opciones_text
            else f"Pregunta: {pregunta}"
        )

        prompt = contexto_texto + "\n\n" + prompt_principal if contexto_texto else prompt_principal

        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ]
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            texto = result['candidates'][0]['content']['parts'][0]['text']
            detalle["respuesta"] = texto.strip()
        except Exception as e:
            logger.error(f"❌ Error con la pregunta '{pregunta}': {e}")
            detalle["respuesta"] = None

        respuestas[pregunta] = detalle

    with open(output_path, "w") as f:
        json.dump({"preguntas": respuestas}, f, indent=2)

    logger.info(f"✅ Respuestas guardadas en {output_path}")

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
    logger.info(f"ℹ️ Current UTC time: {now.isoformat()}")
    logger.info(f"⏳ Threshold (min_diff_ejecucion_bot): {minutes} minutes ago = {threshold_time.isoformat()}")
    logger.info(f"ℹ️ Checking for active DAG runs in the past {minutes} minutes (since {threshold_time.isoformat()})...")
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

    logger.info(f"ℹ️ Total recent executions with non-skipped tasks: {len(recent_runs)}")

    if recent_runs:
        most_recent = max([run.execution_date for run in recent_runs])
        minutes_passed = int((now - most_recent).total_seconds() // 60)
        minutes_remaining = minutes - minutes_passed
        logger.info(f"⏳ Time since last run: {minutes_passed} minutes. Remaining: {minutes_remaining} minutes.")

    if recent_runs:
        logger.warning("⚠️ A recent active execution exists. Skipping this DAG run.")
        return False

    logger.info("✅ No recent active executions. Proceeding with DAG run.")
    return True




# --- Sensor to check for no recent builds ---

@provide_session
def no_recent_builds(session=None, **kwargs):

    logger = LoggingMixin().log
    now = utcnow()
    lock_key = "build_lock_timestamp"
    minutes = int(Variable.get("espera_ejecucion_bot", default_var=5))
    wait_seconds = minutes * 60

    last_timestamp_str = Variable.get(lock_key, default_var="2000-01-01T00:00:00")
    from datetime import timezone
    last_timestamp = datetime.fromisoformat(last_timestamp_str).replace(tzinfo=timezone.utc)
    delta = (now - last_timestamp).total_seconds()

    if delta < wait_seconds:
        wait_remaining = wait_seconds - delta
        logger.info(f"⏳ Locked: last build started at {last_timestamp.isoformat()}. Waiting {int(wait_remaining)}s more.")
        return False

    Variable.set(lock_key, now.isoformat())
    logger.info(f"✅ Lock acquired. This task will proceed. New lock until { (now + timedelta(seconds=wait_seconds)).isoformat() }")
    return True


@provide_session
def build_docker_image(session=None):
    logger = LoggingMixin().log

    logger.info(f"🧹 Verificando ruta bot_path: {bot_path}")
    logger.info(f"🧹 Verificando existencia de Dockerfile en: {os.path.join(bot_path, 'Dockerfile')}")
    logger.info(f"🧹 Verificando existencia de entrypoint.sh en: {os.path.join(bot_path, 'entrypoint.sh')}")
    assert os.path.exists(os.path.join(bot_path, "Dockerfile")), "🚫 Dockerfile no encontrado en bot_path"
    #assert os.path.exists(os.path.join(bot_path, "entrypoint.sh")), "🚫 entrypoint.sh no encontrado en bot_path"

    build_command = ["docker", "build",
                     #
                     #"--dns=8.8.8.8",
                     "--network", "host",
                     "-t", "l-bot-custom", "."]
    logger.info(f"🛠️ Building Docker image with command: {' '.join(build_command)}")
    logger.info(f"🛠️ [COMMAND] {' '.join(build_command)}")

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
            logger.info(f"ℹ️ {line}")

        logger.info("✅ Docker image built successfully.")

    except subprocess.TimeoutExpired as e:
        logger.error(f"❌ Docker build timed out after 30 minutes: {e}")
        raise
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Docker build failed with return code {e.returncode}")
        logger.error("❌ Build output:")
        if e.output:
            for line in e.output.splitlines():
                logger.error(f"❌ {line}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error during Docker build: {str(e)}")
        raise

    return True





def schedule_reboot(host_name):
    logger = LoggingMixin().log
    reboot_command = ["ssh", "-vvv", host_name, "sudo reboot"]
    logger.info(f"🛠️ [COMMAND] {' '.join(reboot_command)}")
    if host_name.startswith("s0"):
        logger.info(f"ℹ️ Skipping reboot for {host_name} (starts with 's0').")
        return
    logger.info(f"🛠️ Rebooting {host_name}...")
    result = subprocess.run(reboot_command, capture_output=True, text=True)
    logger.info(f"ℹ️ [reboot command] STDOUT:\n{result.stdout}")
    logger.info(f"ℹ️ [reboot command] STDERR:\n{result.stderr}")
    time.sleep(20)



def run_docker(host_name, **context):
    logger = LoggingMixin().log
    # Cleanup /tmp/{hostname}*/ immediately before running the container
    cleanup_command = f'rm -rf /tmp/{host_name}*/*'
    logger.info(f"🧹 [CLEANUP] Running: {cleanup_command}")
    subprocess.run(cleanup_command, shell=True, check=False)

    # Crear red Docker personalizada si el host no empieza con "s0-"
    custom_network = f"net_{host_name}"
    port_offset = int(host_name[-1])  # extrae 1 de 's1'
    if not host_name.startswith("s0-"):
        logger.info(f"🛠️ Creando red Docker personalizada: {custom_network}")
        subnet = f"172.28.{100 + port_offset}.0/24"
        subprocess.run([
            "docker", "network", "create",
            "--driver", "bridge",
            "--subnet", subnet,
            custom_network
        ], check=False)


    if host_name.startswith("s0"):
        ports = [
            f"{4443 + port_offset}:4444",
            f"{5899 + port_offset}:5900",
            f"{7899 + port_offset}:7900"
        ]
        #if host_name == "s0-1":
        #    port_offset=-1
        #else:
        #    port_offset=0
        puertos_en_uso = ["-p", ports[0], "-p", ports[1], "-p", ports[2]]
        extra_caps = ["-d"]
        variables_en_uso = [
            "-e", f"SE_VNC_PORT=5900"
            #"-e", f"SE_VNC_NO_PASSWORD=1"
        ]
    if not host_name.startswith("s0"):
        ports = [
            f"{4454 + port_offset}:4444",
            f"{5909 + 10+ port_offset}:5900",
            f"{7909 +10+  port_offset}:7900"
        ]
        extra_caps = ["-d", "--cap-add=NET_ADMIN", "--add-host", "host.docker.internal:host-gateway"]
        puertos_en_uso = ["-p", ports[1]]
        variables_en_uso = [
            #"-e", f"SE_VNC_NO_PASSWORD=1",
            "-e", f"SE_VNC_PORT={5909 +10+ port_offset}",
            #"-e", f"SE_VNC_PORT={4454 +10+ port_offset}",
            "-e", f"SE_NO_VNC_PORT={7909 +10+ port_offset}"

        ]


    host_index = HOSTS.index(host_name)
    cpu_primary = host_index
    cpu_secondary = 7 - host_index
    cpu_limit = [#"--cpuset-cpus",
                 #f"{cpu_primary},{cpu_secondary}",
                 "--cpus=8"
                 ]
    # run_command (ajustar network según host_name)
    run_command = [
        "docker", "run",
        "--rm",
        # "-d",
        *extra_caps,
        *cpu_limit, 
        *variables_en_uso,
        "--name", f"l-bot-{host_name}",
        "--network", custom_network if not host_name.startswith("s0-") else "bridge",
        "--hostname", f"d-{host_name}" if not host_name.startswith("s0-") else host_name,
        *puertos_en_uso,
        "--ulimit", "nofile=32768",
        #"--tmpfs", "/tmp:rw,size=1024m",
        "--shm-size", "2g",
        "-v", f"{bot_path}:/home/seluser/compartido",
        "l-bot-custom"
    ]

    logger.info(f"🛠️ [COMMAND] {' '.join(run_command)}")
    result = subprocess.run(run_command, capture_output=True, text=True)
    logger.info(f"ℹ️ [docker run] STDOUT:\n{result.stdout}")
    logger.info(f"ℹ️ [docker run] STDERR:\n{result.stderr}")
    result.check_returncode()

    segundos = 10

    logger.info(f"⏳ Esperando {segundos} segundos antes de mostrar logs del entrypoint...")
    time.sleep(segundos)

    log_command = ["docker", "logs", f"l-bot-{host_name}"]
    logger.info(f"🛠️ [COMMAND] {' '.join(log_command)}")
    try:
        logs_result = subprocess.run(log_command, capture_output=True, text=True, check=True)
        logger.info(f"ℹ️ [docker logs] STDOUT:\n{logs_result.stdout}")
        logger.info(f"ℹ️ [docker logs] STDERR:\n{logs_result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error al obtener logs del contenedor: {e}")
        logger.error(f"❌ STDOUT:\n{e.stdout}")
        logger.error(f"❌ STDERR:\n{e.stderr}")





def run_bot_ssh(host_name, **context):
    """
    Runs the bot script (main.py) inside the running Docker container 'l-bot'.
    Executes main.py within the shared volume path.
    """
    logger = LoggingMixin().log
    container_name = f"l-bot-{host_name}"

    if not host_name.startswith("s0"):
        sshuttle_cmd = [
            "docker", "exec", "-u", "root", container_name,
            "bash", "-c",
            f"sshuttle --ssh-cmd 'ssh -J yo0' -r {host_name} 0.0.0.0/0 --daemon --pidfile /tmp/sshuttle_{host_name}.pid"
        ]
        logger.info(f"🛠️ [COMMAND] {' '.join(sshuttle_cmd)}")
        try:
            result = subprocess.run(sshuttle_cmd, capture_output=True, text=True)
            logger.info(f"ℹ️ [sshuttle stdout]\n{result.stdout}")
            logger.info(f"ℹ️ [sshuttle stderr]\n{result.stderr}")
            logger.info(f"✅ sshuttle launched in background for {host_name}")
        except Exception as e:
            logger.error(f"❌ Error launching sshuttle on host: {e}")
            raise

        time.sleep(10)  # Esperar a que sshuttle esté listo
    result = subprocess.run(
        ["curl","-4", "-s", "ifconfig.me"],
        capture_output=True, text=True, check=True
    )
    current_ip = result.stdout.strip()
    logger.info(f"🌐 IP pública desde fuera del contenedor {container_name}: {current_ip}")


    puerto_vnc = int(host_name[-1]) 

    ssh_cmd = [
    "docker", "exec", "-u", "root", container_name,
    "bash", "-c",
    f"ssh -N -f -R {5909 + 10 + puerto_vnc}:localhost:5900 yo0"    ]
    #logger.info(f"🛠️ [COMMAND] {' '.join(ssh_cmd)}")
    #try:
        #result = subprocess.run(ssh_cmd, capture_output=True, text=True)
        #logger.info(f"ℹ️ [ssh stdout]\n{result.stdout}")
        #logger.info(f"ℹ️ [ssh stderr]\n{result.stderr}")
        #logger.info(f"✅ Ssh launched in port {5909 + 10 + puerto_vnc} for {host_name}")
    #except Exception as e:
        #logger.error(f"❌ Error launching ssh on port {5909 +10+ puerto_vnc} on host: {e}")
        #raise



    try:
        container_name = f"l-bot-{host_name}"
        result = subprocess.run(
            ["docker", "exec", container_name, "curl", "-s", "ifconfig.me"],
            capture_output=True, text=True, check=True
        )
        current_ip = result.stdout.strip()
        logger.info(f"🌐🌐🌐 IP pública desde contenedor {container_name}: {current_ip}")
    except Exception as e:
        logger.error(f"❌ Error obteniendo IP pública desde contenedor: {e}")
        raise

    ip_map_raw = Variable.get("ips_autorizadas_bot", default_var="", deserialize_json=False)
    ip_map = dict(line.strip().split("=") for line in ip_map_raw.strip().splitlines() if "=" in line)

    expected_host = "s0" if host_name.startswith("s0") else host_name
    ip_ok = any(ip == current_ip and nombre == expected_host for ip, nombre in ip_map.items())


    expected_ip = next((ip for ip, nombre in ip_map.items() if nombre == expected_host), "desconocida")
    if not ip_ok:
        logger.warning(f"⚠️ La IP actual ({current_ip}) no coincide con la esperada para {host_name} (la esperada es {expected_ip}). Abortando ejecución.")
        #time.sleep(200)
        return



    tiempo_minimo=MIN
    tiempo_maximo=MAX

    main_command = [
        "docker", "exec", "-u", "seluser", "-w", f"/home/seluser/compartido", f"l-bot-{host_name}",
        "python3", "main.py",
        "--config", f"json/conf-{host_name}.json",
        "--responses", f"json/resp-s0.json" if host_name.startswith("s0") else f"json/resp-{host_name}.json",
        "--IP", f"{expected_ip}",
        str(tiempo_minimo), str(tiempo_maximo)
    ]

    logger.info(f"🛠️ [COMMAND] {' '.join(main_command)}")

    process = subprocess.Popen(main_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

    for line in iter(process.stdout.readline, ''):
        if line:
            logger.info(f"ℹ️ {line.strip()}")

    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, main_command)

    logger.info("✅ Bot script completed successfully.")



    # Detener sshuttle dentro del contenedor
    stop_sshuttle_cmd = [
        "docker", "exec", "-u", "seluser", container_name,
        "bash", "-c", "kill $(cat /tmp/sshuttle_s1.pid) && rm /tmp/sshuttle_s1.pid"
    ]

    logger.info(f"🛠️ [COMMAND] {' '.join(stop_sshuttle_cmd)}")
    try:
        subprocess.run(stop_sshuttle_cmd, check=True)
        logger.info("🧹 sshuttle daemon stopped in container.")
    except Exception as e:
        logger.warning(f"⚠️ No se pudo detener sshuttle en contenedor: {e}")






def stop_bot_ssh(host_name):
    """
    Stops the Docker container l-bot.
    Logs both stdout and stderr output.
    """
    logger = LoggingMixin().log
    logger.info(f"🛠️ Running stop command: docker stop l-bot-{host_name}")
    logger.info(f"🛠️ [COMMAND] docker stop l-bot-{host_name}")
    result = subprocess.run(["docker", "stop", f"l-bot-{host_name}"], capture_output=True, text=True)    
    logger.info(f"ℹ️ [docker stop l-bot] STDOUT:\n{result.stdout}")
    logger.info(f"ℹ️ [docker stop l-bot] STDERR:\n{result.stderr}")
    result.check_returncode()


def remove_docker_network(host, **kwargs):
    logger = LoggingMixin().log
    network_name = f"net_{host}"
    logger.info(f"🧹 Intentando eliminar red Docker personalizada: {network_name}")
    result = subprocess.run(["docker", "network", "rm", network_name], capture_output=True, text=True)
    if result.returncode == 0:
        logger.info(f"✅ Red eliminada correctamente: {network_name}")
    else:
        logger.warning(f"⚠️ No se pudo eliminar la red {network_name} (puede que ya no exista).")
        logger.warning(f"STDERR: {result.stderr.strip()}")



def run_and_stream_ssh(cmd, logger):
    logger.info(f"🛠️ Running command: {cmd}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, shell=isinstance(cmd, str))
    for line in iter(process.stdout.readline, ''):
        logger.info(f"ℹ️ {line.strip()}")
    process.stdout.close()
    returncode = process.wait()
    if returncode != 0:
        raise subprocess.CalledProcessError(returncode, cmd)


def nada():
    print("Nada")



# --- MULTI-DAG GENERATION ---
dags = {}

for host in HOSTS:
    if host=='s0-1':
        subfix='_es'
    elif host=='s0-2':
        subfix='_co'
    elif host=='s0-3':
        subfix='_me'
    elif host=='s0-4':
        subfix='_la'
    elif host=='s1':
        subfix='_vi'
    elif host=='s3':
        subfix='_ja'
    else:
        subfix=''

    dag_id = f"SERVER_bot_controller_{host}{subfix}"

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        #schedule_interval="@hourly",
        #schedule_interval="@daily",
        schedule_interval="* */4 * * *",
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

        # Insert PythonSensor to wait for no recent builds
        wait_for_idle_build = PythonSensor(
            task_id="wait_for_idle_build",
            python_callable=no_recent_builds,
            poke_interval=120,
            timeout=5400,
            mode="poke"
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

        call_llm_task = PythonOperator(
            task_id=f"{host}_call_llm_t",
            python_callable=call_gemini_llm,
            retries=1,
            op_kwargs={"host": host},
            trigger_rule="all_done"
        )
        # Add schedule_reboot_task before call_llm_task
        schedule_reboot_task = PythonOperator(
            task_id=f"{host}_schedule_reboot_t",
            python_callable=schedule_reboot,
            op_kwargs={"host_name": host},
            retries=1,
            trigger_rule="all_done"
        )
        if not host.startswith("s0"):
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
            # Nueva tarea para eliminar la red personalizada (solo si host no empieza con s0-)
            remove_network_task = PythonOperator(
                task_id=f"{host}_remove_network_t",
                python_callable=remove_docker_network,
                op_kwargs={"host": host},
                trigger_rule="all_done"
            )

        if host.startswith("s0"):

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
        # Update task sequence to include wait_for_idle_build before docker_build_image
        if host.startswith("s0"):
            check_if_should_run >> wait_for_idle_build >> docker_build_image >> run_docker_task >> run_bot_task >> docker_down >> s0_prune_log_t >> schedule_reboot_task >> call_llm_task

        if not host.startswith("s0"):
            check_if_should_run >> wait_for_idle_build >> docker_build_image >> run_docker_task >> run_bot_task >> docker_down >> remove_network_task >> docker_prune_and_log_freed_space >> schedule_reboot_task >> call_llm_task



# Expose all generated DAGs to Airflow
globals().update(dags)
