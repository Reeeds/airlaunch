
from datetime import datetime, timedelta
import pandas as pd
import io
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow.utils.db import provide_session
from airflow.models import XCom
from sqlalchemy import func


#test

default_args = {
    'owner': 'airflow',
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}

@provide_session
def cleanup_xcom(session=None):
    print('hoi')
    session.query(XCom).filter(XCom.execution_date <= func.date('2025-06-01')).delete(synchronize_session=False)



@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2),on_success_callback=cleanup_xcom())
def delete_xcoms():

    @task()
    def loadData():
        print('hoi')

    loadData()

pre = pre()

