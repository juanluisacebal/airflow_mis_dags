import os
import subprocess
import smtplib
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import json
from datetime import datetime, timedelta
from airflow.models import DagRun, Variable
import logging
from airflow.utils.timezone import utcnow
from airflow.utils.state import State
from airflow.utils.db import provide_session


default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))

notification_emails = Variable.get(
    "email_notification",
    default_var="airflow@juanluisacebal.com"
).split(",")

BASE_PATH = os.path.join(Variable.get("ruta_files"), "temp")
os.makedirs(BASE_PATH, exist_ok=True)

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
            DagRun.dag_id == "SERVER_free_space",
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
                TaskInstance.task_id == "email_report",
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



def check_min_free_space(**kwargs):
    minimo_espacio = int(Variable.get("minimo_espacio", default_var="5"))

    result = subprocess.run("df /", shell=True, capture_output=True, text=True)
    lines = result.stdout.strip().split("\n")
    usage_line = lines[1] if len(lines) > 1 else ""
    logging.info(f"🔍 Disk usage line: {usage_line}")
    try:
        percent_str = usage_line.split()[4]  # e.g. "82%"
        usage_percent = int(percent_str.strip('%'))
        logging.info(f"📦 Current disk usage: {usage_percent}%")
        critical_threshold = 100 - minimo_espacio
        warning_threshold = critical_threshold - 1

        if usage_percent >= critical_threshold:
            logging.error(f"❌ Disk usage critical ({usage_percent}%)! Sending alert and shutting down system.")
            send_email(
                f"🚨 CRITICAL: Disk usage has reached {usage_percent}%. The server is shutting down.",
                html_content=False
            )
            subprocess.run("sudo shutdown -h +15", shell=True)
        elif usage_percent >= warning_threshold:
            logging.warning(f"⚠️ Disk usage approaching critical limit: {usage_percent}% (threshold: {critical_threshold}%)")
            send_email(
                f"⚠️ WARNING: Disk usage is at {usage_percent}%. Server will shut down at {critical_threshold}%.",
                html_content=False
            )
        else:
            logging.info(f"✅ Disk usage within limits ({usage_percent}%).")
    except Exception as e:
        logging.error(f"⚠️ Error parsing disk usage: {e}")

def generate_report(**kwargs):
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    current_file = os.path.join(BASE_PATH, f"{today_str}_disk_root.txt")
    logging.info("📁 Starting disk usage report generation.")
    logging.info(f"Output file will be: {current_file}")

    # Run du
    du_command = "sudo du -h --max-depth=4 / 2>/dev/null | sort -hr | head -n 70"
    logging.info(f"Running command: {du_command}")
    result = subprocess.run(du_command, shell=True, capture_output=True, text=True)
    logging.info("Command executed, writing result to file.")
    with open(current_file, "w") as f:
        f.write(result.stdout)
    logging.info("Disk usage report generation complete.")

    kwargs['ti'].xcom_push(key='current_file', value=current_file)

def load_reports(**kwargs):
    ti = kwargs['ti']
    logging.info("📂 Loading report files from disk.")
    prev_files = sorted(
        [f for f in os.listdir(BASE_PATH) if f.endswith(".txt")],
        reverse=True
    )
    logging.info(f"Found files: {prev_files}")
    prev_file = os.path.join(BASE_PATH, prev_files[1]) if len(prev_files) > 1 else None
    logging.info(f"Selected previous file: {prev_file}")
    current_file = ti.xcom_pull(key='current_file')
    logging.info(f"Current file from XCom: {current_file}")

    return prev_file, current_file

def compute_changes(**kwargs):
    ti = kwargs['ti']
    prev_file, current_file = load_reports(**kwargs)
    logging.info("🔍 Starting computation of changes.")
    logging.info(f"Previous report: {prev_file}")
    logging.info(f"Current report: {current_file}")

    if not prev_file or not os.path.exists(prev_file):
        logging.info("📁 First report generated. No previous data to compare. No email sent.")
        return

    def parse_du(file_path):
        data = {}
        with open(file_path, "r") as f:
            for line in f:
                try:
                    size, path = line.strip().split(None, 1)
                    data[path] = size
                except:
                    continue
        return data

    def human_to_bytes(h):
        units = {"K": 1024, "M": 1048576, "G": 1073741824, "T": 1099511627776}
        h = h.replace(",", ".")
        if h[-1] in units:
            return float(h[:-1]) * units[h[-1]]
        return float(h)

    logging.info("Parsing previous report.")
    prev_data = parse_du(prev_file)
    logging.info("Parsing current report.")
    curr_data = parse_du(current_file)

    changes = []
    logging.info("Calculating size differences and percentages.")
    for path in curr_data:
        if path in prev_data:
            curr_b = human_to_bytes(curr_data[path])
            prev_b = human_to_bytes(prev_data[path])
            diff = curr_b - prev_b
            perc = (diff / prev_b) * 100 if prev_b else 0
            changes.append((path, prev_data[path], curr_data[path], perc))

    logging.info(f"Total matched paths: {len(changes)}")
    logging.info("Sorting changes by absolute percentage.")
    changes.sort(key=lambda x: abs(x[3]), reverse=True)
    changes = changes[:50]

    ti.xcom_push(key='changes', value=changes)

def build_html_table(**kwargs):
    ti = kwargs['ti']
    changes = ti.xcom_pull(key='changes')

    if not changes:
        logging.info("📄 No changes to render. Skipping HTML table generation.")
        return

    logging.info("🧱 Building HTML table from changes.")
    logging.info(f"Total changes to render: {len(changes)}")
    
    html = """
    <html><body><h2>📊 Disk Usage Change Report</h2>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%; max-width: 800px;">
    <thead><tr><th>Path</th><th>Previous</th><th>Current</th><th>Change</th></tr></thead><tbody>
    """
    for path, prev, curr, perc in changes:
        color = "red" if perc > 5 else "green" if perc < -5 else "black"
        arrow = "↑" if perc > 0 else "↓" if perc < 0 else "→"
        html += f"<tr><td>{path}</td><td>{prev}</td><td>{curr}</td><td style='color:{color}'>{arrow} {perc:.2f}%</td></tr>"
    html += "</tbody></table></body></html>"

    # Append disk usage summary
    df_output = subprocess.run("df -h /", shell=True, capture_output=True, text=True).stdout.strip().split('\n')
    if len(df_output) > 1:
        html += "<br><h3>💽 Disk Space Summary (df -h)</h3><pre>" + "\n".join(df_output) + "</pre>"

    # Append memory usage table
    free_output = subprocess.run("free -h", shell=True, capture_output=True, text=True).stdout.strip().split('\n')
    html += "<br><h3>🧠 Memory Usage (free -h)</h3><table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>"
    # Normalize free output with consistent memory labels
    memory_labels = ["Tipo", "total"] + free_output[0].split()[1:]
    html += "<tr>" + "".join(f"<th>{label}</th>" for label in memory_labels) + "</tr>"
    for line in free_output[1:]:
        cols = line.split()
        if not cols:
            continue
        html += "<tr><th>{}</th><td>{}</td>".format(cols[0], cols[1]) + "".join(f"<td>{col}</td>" for col in cols[2:]) + "</tr>"
    html += "</table>"

    ti.xcom_push(key='html_content', value=html)
    logging.info("HTML table created and pushed to XCom.")


def email_report(**kwargs):
    ti = kwargs['ti']
    logging.info("📬 Preparing to send report email.")
    logging.info("Retrieving HTML content from XCom.")
    html_content = ti.xcom_pull(key='html_content')

    if not html_content:
        logging.info("📪 No HTML content found. Skipping email.")
        return

    logging.info("Calling send_email() with HTML content.")
    send_email(html_content, html_content=True)

    

def send_email(body, html_content=False):
    from smtplib import SMTP
    import ssl

    conn = BaseHook.get_connection("smtp_default")
    msg = MIMEText(body, "html" if html_content else "plain")
    msg["Subject"] = f"[Airflow] Disk Usage Report {datetime.now().strftime('%Y-%m-%d')}"
    msg["From"] =  "contacto@juanluisacebal.com"
    msg["To"] = ", ".join(notification_emails)

    logging.info("✉️ Sending email using smtp_default connection.")
    logging.info(f"From: {msg['From']}, To: {msg['To']}")
    logging.info(f"Subject: {msg['Subject']}")
    logging.info("Connecting to SMTP server with STARTTLS...")

    with SMTP(conn.host, conn.port) as server:
        server.ehlo()
        context = ssl.create_default_context()
        server.starttls(context=context)
        server.ehlo()
        logging.info("Logging in...")
        server.login(conn.login, conn.password)
        logging.info("Sending email...")
        server.sendmail(msg["From"], notification_emails, msg.as_string())
    logging.info("Email sent successfully.")

with DAG(
    dag_id="SERVER_free_space",
    start_date=days_ago(2900),
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    #schedule_interval="0 6 */2 * *",  # Every 2 days at 6am
    tags=["monitoring"],
    doc_md="""
    ### 📊 SERVER_free_space DAG Documentation
    
    This DAG performs disk usage analysis and system safety checks every two days.
    
    #### 🧩 Components:
    
    - **check_min_free_space**: 
      - Checks the root (`/`) disk space.
      - If usage exceeds a critical threshold (`minimo_espacio` Airflow Variable, default 5%), it sends an alert and shuts down the system.
      - Sends a warning email if usage is 1% below threshold.
    
    - **generate_report**:
      - Runs `du -h --max-depth=4 /` and stores the top 30 paths by size.
      - Stores output with a timestamp.
    
    - **compute_changes**:
      - Compares the latest report with the previous one.
      - Calculates size and percentage changes for each common path.
    
    - **build_html_table**:
      - Builds a responsive HTML table showing the top 20 changes sorted by absolute % change.
    
    - **email_report**:
      - Sends the table as an HTML email using the `smtp_default` connection.
      - From: contacto@juanluisacebal.com
      - To: airflow@juanluisacebal.com
    
    #### ⚙️ Configuration:
    
    - Airflow Variable: `ruta_files` (path to store report files)
    - Airflow Variable: `minimo_espacio` (percentage of minimum free space required)
    
    #### ⏱️ Schedule:
    - Runs every 2 days at 6:00 AM (`0 6 */2 * *`)
    """
) as dag:
    check_min_free_space_task = PythonOperator(
        task_id="check_min_free_space",
        python_callable=check_min_free_space,
        provide_context=True
    )
    check_min_free_space_task.doc_md = "Checks available disk space and shuts down system if usage is too high."

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        provide_context=True
    )
    generate_report_task.doc_md = "Generates a disk usage report using `du` and stores it with today's timestamp."

    compute_changes_task = PythonOperator(
        task_id="compute_changes",
        python_callable=compute_changes,
        provide_context=True
    )
    compute_changes_task.doc_md = "Parses current and previous reports, computes size and percentage differences."

    build_html_table_task = PythonOperator(
        task_id="build_html_table",
        python_callable=build_html_table,
        provide_context=True
    )
    build_html_table_task.doc_md = "Creates a clean HTML report table from the calculated changes."

    email_report_task = PythonOperator(
        task_id="email_report",
        python_callable=email_report,
        provide_context=True
    )
    email_report_task.doc_md = "Sends the HTML report via email using the smtp_default connection."

    check_if_should_run = ShortCircuitOperator(
        task_id="check_if_should_run",
        python_callable=should_run,
    )

    check_min_free_space_task >> check_if_should_run >> generate_report_task
    generate_report_task >> compute_changes_task >> build_html_table_task >> email_report_task