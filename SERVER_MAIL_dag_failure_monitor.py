"""
DAG: SERVER_MAIL_dag_failure_monitor

Monitors recent DAG failures and sends email alerts when necessary.
The notification logic avoids spam by sending only if:
  - There are new failures,
  - Or at least 12 hours have passed since the last notification.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.session import provide_session
from airflow.models import DagRun, DagModel, Variable
from airflow.utils import timezone
import pendulum
from smtplib import SMTP
import ssl
from email.mime.text import MIMEText
from airflow.hooks.base_hook import BaseHook
#from airflow.plugins.email_con_delay import EmailOperatorConDelay


default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))


class EmailOperatorWithLog(EmailOperator):
     """
     Custom EmailOperator that logs recipient, subject, and content details before sending.
     """
     def __init__(self, smtp_conn_id=None, *args, **kwargs):
         if smtp_conn_id:
             kwargs['conn_id'] = smtp_conn_id
         super().__init__(*args, **kwargs)
 
     def execute(self, context):
         logging.info("Starting EmailOperatorWithLog...")
         logging.info("Recipients: %s", self.to)
         logging.info("Subject: %s", self.subject)
         logging.info("HTML Content:\n%s", self.html_content)
         result = super().execute(context)
         logging.info("Email sent successfully. Result: %s", result)
         return result
 
 

# Configuration values
HOURS_WINDOW = 48
DAYS_SHORT = 7
DAYS_LONG = 30

notification_emails = Variable.get(
    "email_notification",
    default_var="airflow@juanluisacebal.com"
).split(",")


def initialize_notification_variables():
    """
    Ensure Airflow Variables for failure tracking exist.
    If not found, creates them with default values.
    - AIRFLOW_MONITOR_FAILED_DAGs_prev_hours: previous failures list (JSON)
    - AIRFLOW_MONITOR_FAILED_DAGs_last_email: ISO timestamp of last sent alert
    """
    try:
        Variable.get("AIRFLOW_MONITOR_FAILED_DAGs_prev_hours", deserialize_json=True)
        logging.info("Variable AIRFLOW_MONITOR_FAILED_DAGs_prev_hours found.")
    except Exception:
        default_prev = ["A", "B", "C"]
        Variable.set("AIRFLOW_MONITOR_FAILED_DAGs_prev_hours", default_prev, serialize_json=True)
        logging.info("Variable AIRFLOW_MONITOR_FAILED_DAGs_prev_hours not found; default assigned: %s", default_prev)

    try:
        Variable.get("AIRFLOW_MONITOR_FAILED_DAGs_last_email")
        logging.info("Variable AIRFLOW_MONITOR_FAILED_DAGs_last_email found.")
    except Exception:
        default_date = "1900-01-01T00:00:00+00:00"
        Variable.set("AIRFLOW_MONITOR_FAILED_DAGs_last_email", default_date)
        logging.info("Variable AIRFLOW_MONITOR_FAILED_DAGs_last_email not found; default assigned: %s", default_date)

@provide_session
def get_failed_dags(session=None, delta=None, filter_active=False):
    """
    Retrieve DAGs that have failed at least once within a given delta period,
    and whose most recent execution is still marked as failed.

    Parameters:
        - session: Airflow DB session (injected)
        - delta: timedelta object for lookback window
        - filter_active: if True, ignore paused DAGs

    Returns:
        List of DAG IDs.
    """
    limit_date = timezone.utcnow() - delta
    logging.info("Querying failed DAGs since %s", limit_date)

    failed_runs = session.query(DagRun).filter(
        DagRun.state == 'failed',
        DagRun.execution_date >= limit_date
    ).all()
    logging.info("Found %d failed runs.", len(failed_runs))

    dag_ids = {run.dag_id for run in failed_runs}
    logging.info("Failed DAGs in the period: %s", list(dag_ids))

    final_dag_ids = []
    for dag_id in dag_ids:
        last_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id
        ).order_by(DagRun.execution_date.desc()).first()

        if last_run:
            logging.info("DAG %s: last execution %s, state: %s", dag_id, last_run.execution_date, last_run.state)
            if last_run.state == 'failed':
                final_dag_ids.append(dag_id)
            else:
                logging.info("DAG %s is not currently failed. Discarded.", dag_id)
        else:
            logging.info("No runs found for DAG %s", dag_id)

    if filter_active:
        active_dags = []
        for dag_id in final_dag_ids:
            model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
            if model:
                logging.info("DAG %s: is_paused=%s", dag_id, model.is_paused)
                if not model.is_paused:
                    active_dags.append(dag_id)
                else:
                    logging.info("DAG %s is paused, skipped.", dag_id)
            else:
                active_dags.append(dag_id)
        final_dag_ids = active_dags
        logging.info("Active failed DAGs: %s", final_dag_ids)

    return final_dag_ids

@provide_session
def get_total_failed_lists(session=None, **context):
    """
    Build and return categorized failed DAGs list based on different time windows.

    Returns:
        Dictionary with:
        - recent_failed: active failures in last HOURS_WINDOW
        - short_term_failed: active failures in last DAYS_SHORT
        - long_term_failed: failures in last DAYS_LONG excluding short_term_failed
    """
    recent_failed = get_failed_dags(delta=timedelta(hours=HOURS_WINDOW), filter_active=True)
    short_term_failed = get_failed_dags(delta=timedelta(days=DAYS_SHORT), filter_active=True)
    all_30d = get_failed_dags(delta=timedelta(days=DAYS_LONG), filter_active=False)
    long_term_failed = [dag for dag in all_30d if dag not in short_term_failed]

    return {
        'recent_failed': recent_failed,
        'short_term_failed': short_term_failed,
        'long_term_failed': long_term_failed
    }

def get_previous_recent_failures() -> list:
    """
    Load previous recent failures from Airflow Variables.

    Returns:
        List of DAG IDs, or None if variable not found.
    """
    try:
        prev = Variable.get("AIRFLOW_MONITOR_FAILED_DAGs_prev_hours", deserialize_json=True)
        logging.info("Previous failed DAGs: %s", prev)
        return prev
    except Exception:
        logging.info("Previous variable not found.")
        return None

def get_last_email_sent():
    """
    Get the timestamp of the last sent alert email.

    Returns:
        datetime object, or None if missing.
    """
    try:
        date_str = Variable.get("AIRFLOW_MONITOR_FAILED_DAGs_last_email")
        logging.info("Last email date: %s", date_str)
        return timezone.parse(date_str)
    except Exception:
        logging.info("Last email variable not found.")
        return None

def update_notification_state(current: list):
    """
    Update Airflow Variables with current failure list and email timestamp.
    """
    Variable.set("AIRFLOW_MONITOR_FAILED_DAGs_prev_hours", current, serialize_json=True)
    Variable.set("AIRFLOW_MONITOR_FAILED_DAGs_last_email", timezone.utcnow().isoformat())
    logging.info("Notification state updated. New failures: %s", current)

def should_send_email(current: list, previous: list, last_sent) -> bool:
    """
    Determine if an alert email should be sent.

    Returns:
        True if an email should be sent, otherwise False.
    """
    if not current:
        logging.info("No active failures in current list; email will not be sent.")
        return False
    if previous is None or len(previous) == 0:
        logging.info("No previous list or empty; email will be sent.")
        return True
    if set(current) != set(previous):
        logging.info("Current list differs from previous; email will be sent.")
        return True
    if last_sent is None:
        logging.info("No record of last email; email will be sent.")
        return True
    if timezone.utcnow() - last_sent >= timedelta(hours=12):
        logging.info("At least 12 hours have passed since last email; email will be sent.")
        return True
    logging.info("Conditions not met for sending email.")
    return False

def email_condition(**context):
    """
    ShortCircuitOperator condition.
    Ensures alert email is only sent:
      - After 7 AM local time
      - If failure list changed or 12h passed
    """
    exec_date = context['execution_date']
    local_dt = exec_date.in_timezone("America/Montevideo")
    logging.info("Local execution time: %s", local_dt)

    if local_dt.hour < 7:
        logging.info("Before 7 AM; email will not be sent.")
        return False

    current_data = context['ti'].xcom_pull(task_ids='get_failed_lists')
    current_list = current_data.get('recent_failed', [])
    previous_list = get_previous_recent_failures()
    last_sent = get_last_email_sent()

    if should_send_email(current_list, previous_list, last_sent):
        update_notification_state(current_list)
        return True
    return False



html_template = """
{% set data = ti.xcom_pull(task_ids='get_failed_lists') %}
{% set recent_failed = data['recent_failed'] %}
{% set short_term_failed = data['short_term_failed'] %}
{% set long_term_failed = data['long_term_failed'] %}

<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; }
    .section-title { color: #2F4F4F; font-size: 18px; text-decoration: underline; margin-bottom: 5px; }
    .subsection { margin-bottom: 20px; }
    ul { list-style-type: square; }
    a { text-decoration: none; color: #0073e6; }
  </style>
</head>
<body>
  <h2 style="color: red;">Attention!</h2>
  <p>Failed DAGs have been detected over different time periods:</p>

  <div class="subsection">
    <p class="section-title"><strong>1. Active failed DAGs in the last {{ HOURS_WINDOW }} hours:</strong></p>
    {% if recent_failed %}
      <ul>
      {% for dag_id in recent_failed %}
        <li><strong><a href="http://airflow.juanluisacebal.com/dags/{{ dag_id }}/grid">{{ dag_id }}</a></strong></li>
      {% endfor %}
      </ul>
    {% else %}
      <p style="font-style: italic;">No failures detected during this period.</p>
    {% endif %}
  </div>

  <div class="subsection">
    <p class="section-title"><strong>2. Active failed DAGs in the last {{ DAYS_SHORT }} days:</strong></p>
    {% if short_term_failed %}
      <ul>
      {% for dag_id in short_term_failed %}
        <li><a href="http://airflow.juanluisacebal.com/dags/{{ dag_id }}/grid">{{ dag_id }}</a></li>
      {% endfor %}
      </ul>
    {% else %}
      <p style="font-style: italic;">No failures detected during this period.</p>
    {% endif %}
  </div>

  <div class="subsection">
    <p class="section-title"><strong>3. Failed DAGs (active and inactive) in the last {{ DAYS_LONG }} days:</strong></p>
    {% if long_term_failed %}
      <ul>
      {% for dag_id in long_term_failed %}
        <li><a href="http://airflow.juanluisacebal.com/dags/{{ dag_id }}/grid">{{ dag_id }}</a></li>
      {% endfor %}
      </ul>
    {% else %}
      <p style="font-style: italic;">No failures detected during this period.</p>
    {% endif %}
  </div>

  <hr>
  <p style="font-style: italic;">
    This message was generated automatically. It is sent only if the list of active failed DAGs in the last {{ HOURS_WINDOW }} hours changes or, if unchanged, at least 12 hours have passed since the last notification.
  </p>
</body>
</html>
"""






html_content = html_template.replace("HOURS_WINDOW", str(HOURS_WINDOW))
html_content = html_content.replace("DAYS_SHORT", str(DAYS_SHORT))
html_content = html_content.replace("DAYS_LONG", str(DAYS_LONG))

with DAG(
    dag_id='SERVER_MAIL_dag_failure_monitor',
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"]
) as dag:

    t_initialize_vars = PythonOperator(
        task_id='initialize_notification_variables',
        python_callable=initialize_notification_variables
    )

    t_get_failed_lists = PythonOperator(
        task_id='get_failed_lists',
        python_callable=get_total_failed_lists,
        provide_context=True
    )

    t_check_notification = ShortCircuitOperator(
        task_id='check_email_condition',
        python_callable=email_condition,
        provide_context=True
    )

    t_send_email = EmailOperatorWithLog(
        task_id='send_failure_email',
        to=notification_emails,
        subject=f'[AIRFLOW ALERTS] Failed DAGs in the last {HOURS_WINDOW} hours',
        html_content=html_content,
        smtp_conn_id='smtp_default'
    )

    t_initialize_vars >> t_get_failed_lists >> t_check_notification >> t_send_email