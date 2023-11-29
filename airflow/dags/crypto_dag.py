from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from bs4 import BeautifulSoup

from datetime import datetime

import requests



def get_crypto_prices():
    result = requests.get('https://coinmarketcap.com/vi/')
    doc = BeautifulSoup(result.text, 'html.parser')
    tbody = doc.tbody
    trs = tbody.contents

    prices = {}

    for tr in trs:
        name, price = tr.contents[2:4]
        print(name.p.string, ":", price.div.string)
        print()




def print_date():

    print('Today is {}'.format(datetime.today().date()))





dag = DAG(

    'crypto_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 7 * * *',

    catchup=False

)



print_crypto_price_task = PythonOperator(

    task_id='print_crypto_prices',

    python_callable=get_crypto_prices,

    dag=dag

)



print_date_task = PythonOperator(

    task_id='print_date',

    python_callable=print_date,

    dag=dag

)






# Set the dependencies between the tasks

print_crypto_price_task >> print_date_task 