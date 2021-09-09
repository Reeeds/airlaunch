
from datetime import datetime, timedelta
import pandas as pd
#import os 
#import glob
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules, apriori
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.utils.email import send_email
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}


minSupport = Variable.get("minSupport")


@dag(default_args=default_args, schedule_interval="* * * * *", start_date=days_ago(2), tags=['example'])
def pre():

    @task()
    def extractData():

        dfDataSalDocsTest = Variable.get("dfDataSalDocsTestJSON", deserialize_json=True)

        dfDataSalDocs = pd.DataFrame(dfDataSalDocsTest)

        dfDataSalDocs = dfDataSalDocs.groupby('SalDoc_InternalNo')['SalDocItem_ArtInternalNo']
        dataSalDocsList = []
        for name, items in dfDataSalDocs:
            basketItems = items.tolist()
            itemsList = []
            for item in basketItems:
                itemsList.append(str(item))
            dataSalDocsList.append(itemsList)
        
        return dataSalDocsList

    @task()
    def transform(data:list):
        te = TransactionEncoder()
        te_ary = te.fit_transform(data, sparse=True)
        sparse_df = pd.DataFrame.sparse.from_spmatrix(te_ary, columns=te.columns_)
        print('Python Script: SparseMatrix calculated')

        frequent_itemsets = apriori(sparse_df, min_support=minSupport, use_colnames=True, max_len=2, low_memory=True) # LowMemory muss True sein!
        #print(frequent_itemsets)
        print('Python Script: Apriori calculated')

        rules = association_rules(frequent_itemsets, metric='lift', min_threshold=1.0)
        print('Python Script: AssociationRules calculated')

        aResult = rules.sort_values('conviction', ascending=False)
        # Umformatieren
        aResult["antecedents"] = rules["antecedents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
        aResult["consequents"] = rules["consequents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
        df = aResult
        print(df.head())





    data = extractData()
    transform(data)

pre = pre()

