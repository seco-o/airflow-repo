from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
}

access_token = "D27yA2mh1cg1J6mVL7JoGPGU6u6B4y8D0469507Kura6k7YptD"

def call_api():
    url = "https://ivtapi.mobildev.in/data/unread/all"
    headers = {
        "Authorization": "Bearer {access_token}"
    }
    try:
        response = requests.get(url, headers=headers)
        # Burada response ile yapmak istediğiniz işlemleri gerçekleştirebilirsiniz.
        # Örneğin, response içeriğini kontrol edebilir veya başka bir işlem gerçekleştirebilirsiniz.
        print("API Response:", response.text)
    except Exception as e:
        print("Error:", e)

with DAG('mobildev_api_unread', 
         default_args=default_args,
         schedule_interval=timedelta(minutes=30),  # Yarım saatte bir çalışacak şekilde ayarlandı.
         catchup=False
         ) as dag:

    call_api_task = PythonOperator(
        task_id='call_api_task',
        python_callable=call_api
    )

    call_api_task

