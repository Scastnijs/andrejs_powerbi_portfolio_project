from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'andrejs',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# schedule_interval='0 0 * * *'
#
with DAG(
    dag_id='dag_with_postgres_operator',
    default_args=default_args,
    start_date=datetime(2024, 1, 20),
    schedule='0 0 * * *'
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_local',
        sql="""
            CREATE TABLE if not exists Orders (
                OrderID INT PRIMARY KEY,
                Status VARCHAR(50)
            )
        """
    )

    '''
    SQLExecuteQueryOperator(
  task_id="run_query",
  conn_id="postgres_default",
  sql="SELECT * FROM users;"
)
    '''

    task2 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='postgres_local',
        sql="""
            insert into Orders values(2,'delivered')
        """
    )

    task1 >> task2