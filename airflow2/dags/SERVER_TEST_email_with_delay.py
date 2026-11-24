from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
#from airflow.plugins.email_con_delay import EmailOperatorConDelay
from airflow.operators.email_operator import EmailOperator

default_args = Variable.get("default_args", deserialize_json=True)
default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
default_args["retry_delay"] = timedelta(minutes=default_args.pop("retry_delay_minutes"))


with DAG("SERVER_TEST_email_with_delay", 
        start_date=datetime(2025, 1, 1),
        #schedule=None,
        default_args=default_args,
        ##schedule_interval=default_args["schedule_interval"],
        schedule_interval='15 6 * * *',

        #schedule_interval="0 7 * * 1-5",
        ) as dag:
    EmailOperator(
        task_id="task_SERVER_TEST_email_with_delay",
        to=["airflow@juanluisacebal.com"],
        subject="Test with Delay",
        html_content="<p>Does this one arrive?</p>",
        conn_id="smtp_default"
    )