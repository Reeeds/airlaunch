
import json
import pandas as pd
from datetime import datetime, timedelta
import pandas_datareader.data as web
import io
import os 
import glob
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

default_args = {
    'owner': 'airflow',
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def stocks_taskflow_api_etl():

    @task()
    def extract():
       
        stock_list = ['AAPL','GOOGL','AMZN','TSLA','FB','ROG.SW','NOVN.SW','IDIA.SW','CSGN.SW','UBSG.SW','RLF.SW','SEDG','MDB','ALGN','ALXN','SIVB','MBTN.SW','PGHN.SW','NESN.SW','ABBV','AYX','ADS']
        result = pd.DataFrame(columns=('Date', 'Stock', 'Action'))
        end = datetime.now()
        start = datetime(end.year - 1,end.month,end.day)#
        for stock in stock_list:   
            globals()[stock] = web.DataReader(stock,'yahoo',start,end)
        for stock in stock_list:
            df = globals()[stock]
            dfClose = df['Close']
            today = dfClose.index[-1]
            yest= dfClose.index[-2]
            close = dfClose[today]
            daily =  (close - dfClose[yest]) / dfClose[yest] * 100
            df['Close: 30 Day Mean'] = df['Close'].rolling(window=20).mean()
            df['Upper'] = df['Close: 30 Day Mean'] + 2*df['Close'].rolling(window=20).std()
            df['Lower'] = df['Close: 30 Day Mean'] - 2*df['Close'].rolling(window=20).std()
            fig = df[['Close','Close: 30 Day Mean','Upper','Lower']].plot(figsize=(16,6),title=stock).get_figure()
            if not os.path.exists('figures'):
                os.makedirs('figures')
            fig.savefig('figures/' + stock + '.png')
            dfTail = df.tail(1)
            date = pd.to_datetime(dfTail.iloc[0].name)
            close = dfTail.iloc[0]['Close']
            upper = dfTail.iloc[0]['Upper']
            lower = dfTail.iloc[0]['Lower']
            diffrence = end - date
            action = 'hold'
            if diffrence.days < 3:
                if close < lower:
                    action='buy'
                elif close > upper:
                    action='sell'
                else:
                    pass
            else:
                print('Date aelter als drei Tage!!!')
            result = result.append({'Date' : date , 'Stock' : stock, 'Action': action,'Close':close,'ChangeToday': daily}, ignore_index=True)
        print(result)
        return result.to_csv(index=False)

    @task()
    def transform(df):
        df = pd.read_csv(io.StringIO(df))     
        print(df)
        return df.to_csv(index=False)

    @task()
    def load(total_order_value: float):
            print("Total order value is: %.2f" % total_order_value)

    @task()
    def email_callback(df):
        df = pd.read_csv(io.StringIO(df))  
        files = glob.glob("figures/*.png")  
        content = df.to_html() 
        send_email(
            to=["reto.schuermann@gmail.com"],
            subject='Report',
            html_content=content,
            files=files
        )
    dataTest = extract()
    dataTest2 = transform(dataTest) 
    email_callback(dataTest2)
   # load(order_summary["total_order_value"])

stocks_etl_dag = stocks_taskflow_api_etl()

