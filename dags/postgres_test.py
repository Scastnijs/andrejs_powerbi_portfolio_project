from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def postgres_test():

    @task
    def test_connection():
        hook = PostgresHook(postgres_conn_id="postgres")

        result = hook.get_first("SELECT version();")

        print(result)

    test_connection()

dag = postgres_test()