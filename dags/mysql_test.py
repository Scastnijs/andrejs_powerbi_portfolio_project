from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def mysql_test():

    @task
    def test_connection():
        hook = MySqlHook(mysql_conn_id="mysql")

        result = hook.get_first("SELECT VERSION();")

        print(result)

    test_connection()

dag = mysql_test()
