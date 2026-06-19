from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

smtp_user = 'andrejsscastnijs@gmail.com'

def print_hello():
    return 'Hello World!'

default_args = {
    'owner': 'Andrejs',
    'start_date':datetime(2026,2,10),
}

with DAG(
    dag_id = 'email_alert_example',
    schedule = None,
    default_args = default_args,
) as dag:

    email = EmailOperator(
        task_id = 'email_alert',
        to = 'andrejsscastnijs@gmail.com',
        subject = 'Email Alert',
        html_content = """ <h3>Email Test</h3>""",
        dag=dag
    )

    dummy_operator = EmptyOperator(
        task_id = 'dummy_task',
        retries = 3,
        dag = dag
    )

    hello_operator = PythonOperator(
        task_id = 'hello_task',
        python_callable = print_hello,
        dag = dag
    )

    email >> dummy_operator >> hello_operator
