from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import pandas_datareader.data as web

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def test():
    print('hoi')
    stock_list = ['AAPL','GOOGL','AMZN','TSLA','FB','ROG.SW','NOVN.SW','IDIA.SW','CSGN.SW','UBSG.SW','RLF.SW','SEDG','MDB','ALGN','ALXN','SIVB','MBTN.SW','PGHN.SW','NESN.SW','ABBV','AYX','ADS']
    result = pd.DataFrame(columns=('Date', 'Stock', 'Action'))
    end = datetime.now()
    start = datetime(end.year - 1,end.month,end.day)#
    test = web.DataReader('AAPL','yahoo',start,end)
    print(test)

with DAG("hoi", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:


    log = PythonOperator(
        task_id="log",
        python_callable=test
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="reto.schuermann@gmail.com",
        subject="Hoi",
        html_content="<h3>Hoi test</h3>"
    )

    log >> send_email_notification