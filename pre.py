
from datetime import datetime, timedelta
import pandas as pd
#import os 
#import glob
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.utils.email import send_email
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 1,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}


@dag(default_args=default_args, schedule_interval="* * * * *", start_date=days_ago(2), tags=['example'])
def pre():

    @task()
    def extractData():
        dfDataSalDocs = pd.read_csv('dfDataSalDocsTest.csv')
        dfDataSalDocs = dfDataSalDocs.groupby('SalDoc_InternalNo')['SalDocItem_ArtInternalNo']
        dataSalDocsList = []
        for name, items in dfDataSalDocs:
            basketItems = items.tolist()
            itemsList = []
            for item in basketItems:
                itemsList.append(str(item))
            dataSalDocsList.append(itemsList)
        
        print(dataSalDocsList[:10])

#    @task()
#    def sendEmail():
#        files = glob.glob("test/*.pdf")  
#        print(files)
#        content = '<h1>Radar</h1><br>' + str(kw) + '<br>' + str(files)
#        send_email(
#            to=receiversList[0],
#            bcc=receiversList[1:],
#            subject='Radar Test ' + str(kw),
#            html_content=content,
#            files=files
#        )
    extractData()

pre = pre()

