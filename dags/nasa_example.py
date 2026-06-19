import json
import pathlib
import airflow
import requests
import requests.exceptions as request_exceptions
from datetime import date
from airflow import DAG
#from airflow.operators.bash import 
from airflow.providers.standard.operators.bash import BashOperator
#from airflow.operators.python import 
from airflow.providers.standard.operators.python import PythonOperator
#from airflow.decorators import task
from airflow.sdk import task
from datetime import datetime, timedelta
dag_owner = 'Andrejs'

def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    #pathlib.Path("C:/images").mkdir(parents=True, exist_ok=True)
    api_key = 'azCEv4d1NnSe66z8fEd0gFbVkamABqIOkNmfYtA6'
    url = f'https://api.nasa.gov/planetary/apod?api_key={api_key}'
    response = requests.get(url).json()
    today_image = response['hdurl']
    with open(f'todays_image_{date.today()}.png', 'wb') as f:
        f.write(requests.get(today_image).content)

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='download_ASOD_image',
          default_args=default_args,
          description='download and notify ',
         #start_date = airflow.utils.dates.days_ago(0),
         start_date = datetime(2026, 2, 10),
          #schedule_interval='@daily',
          schedule='@daily',
          catchup=False,
          tags=['None']
          ):    
    get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    )

notify = BashOperator(
    task_id="notify",
    bash_command='echo f"Image for today has been added!"',
    )

get_pictures >> notify