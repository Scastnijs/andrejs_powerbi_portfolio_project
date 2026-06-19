from airflow.decorators import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def oracle_test():

    @task
    def test_connection():
        hook = OracleHook(oracle_conn_id="oracle")

        result = hook.get_first(
            "SELECT banner FROM v$version"
        )

        print(result)
    test_connection()

dag = oracle_test()