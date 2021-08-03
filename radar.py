
import requests
from datetime import datetime, timedelta
import urllib.request
from scrapy.selector import Selector
baseURL = 'https://polizei.lu.ch'
kw = datetime.now().isocalendar()[1]


#import io
import os 
import glob
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

default_args = {
    'owner': 'airflow',
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 1,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}

@dag(default_args=default_args, schedule_interval="0 16 * * *", start_date=days_ago(2), tags=['example'])
def radar():

    @task()
    def getPdf():
        r = requests.get('https://polizei.lu.ch/organisation/sicherheit_verkehrspolizei/verkehrspolizei/spezialversorgung/verkehrssicherheit/Aktuelle_Tempomessungen')
        body = r.text
        document = Selector(text=body)
        elementsTitle = document.xpath("//a[contains(@target, '_blank')]/text()")
        title1 = elementsTitle[1].get()
        title2 = elementsTitle[2].get()
        print(title1)
        print(title2)
        kw1 = [int(s) for s in title1.split() if s.isdigit()][0]
        kw2 = [int(s) for s in title2.split() if s.isdigit()][0]
        print(kw1)
        print(kw2)

        elements = document.xpath("//a[contains(@target, '_blank')]")

        link1 = baseURL + elements[1].attrib['href']
        link2 = baseURL +elements[2].attrib['href']
        print(link1)
        print(link2)
        
        if not os.path.exists('pdfs'):
            os.makedirs('pdfs')
        urllib.request.urlretrieve(link1, "/pdfs/kw" + str(kw1) + ".pdf")
        urllib.request.urlretrieve(link2, "/pdfs/kw" + str(kw2) + ".pdf")

    @task()
    def sendEmail():
        files = glob.glob("pdfs/*.pdf")  
        content = '<h1>Hoi</h1>'
        send_email(
            to=["reto.schuermann@gmail.com"],
            subject='Radar',
            html_content=content,
            files=files
        )
    getPdf()
    sendEmail()

radar = radar()

