
from datetime import datetime, timedelta
import pandas as pd
#import os 
#import glob
import io
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules, apriori
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.utils.email import send_email
from airflow.models import Variable

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

minSupport = float(Variable.get("minSupport"))
numberOfRecommendationsPerArt = int(Variable.get("numberOfRecommendationsPerArt"))
google_cloud_connection_id = 'google_cloud_default'


@provide_session
def cleanup_xcom(session=None):
    print('hoi')
    session.query(XCom).filter(XCom.execution_date <= func.date('2022-06-01')).delete(synchronize_session=False)



@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['pre'],on_success_callback=cleanup_xcom())
def pre():

    @task()
    def loadData():
        print('hoi')

    loadData()

pre = pre()

