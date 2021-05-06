from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def test():
    print('hoi')


with DAG("hoi", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:


    log = PythonOperator(
        task_id="log",
        python_callable=test
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="reto.schuermann@gmail.com.com",
        subject="Hoi",
        html_content="<h3>Hoi</h3>"
    )

    log >> send_email_notification