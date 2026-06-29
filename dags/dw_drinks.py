from datetime import datetime, timedelta
from airflow import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'andrejs',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def task1(**context):
    hook = PostgresHook(postgres_conn_id="postgres")

    records = hook.get_records("""
        SELECT      
        dc.cust_first_name, 
        dc.cust_last_name,
        dc.cust_city, 
        ds.state_name,
        dc.cust_state 
        
        FROM WSH.DEMO_CUSTOMERS dc
        LEFT JOIN WSH.DEMO_STATES ds ON dc.CUST_STATE = ds.ST
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="customer_data",
        value=records
    )


def task2(**context):
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="customer_data"
    )

    hook = MySqlHook(mysql_conn_id="mysql")

    insert_sql = """
        INSERT INTO customer_location 
            (
                CUST_FIRST_NAME, 
                CUST_LAST_NAME, 
                CUST_COUNTRY,
                CUST_COUNTRY_CODE,
                CUST_CITY,
                CUST_STATE,
                CUST_STATE_CODE
            ) 
            VALUES 
            (
                %s,
                %s,
                'United States',
                'US',
                %s,
                %s,
                %s
            )
    """

    for row in records:
        hook.run(insert_sql, parameters=row)

with DAG(
    dag_id='map_load1',
    default_args=default_args,
    start_date=datetime(2026, 1, 20),
    schedule='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=task1,
    )

    task2 = PythonOperator(
        task_id="task2",
        python_callable=task2,
    )

    task1 >> task2