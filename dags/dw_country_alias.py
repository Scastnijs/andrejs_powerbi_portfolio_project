from datetime import datetime, timedelta
from airflow.sdk import DAG, Param, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow.providers.standard.operators.python import PythonOperator

TABLES = [
    "staging.drinks",
    "staging.expectancy",
    "staging.worldcities",
    "staging.vehicles_country",
    "staging.happiness",
]

default_args = {
    'owner': 'andrejs',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def task1(**context):
    hook = PostgresHook(postgres_conn_id="postgres")
    table = context["params"]["table"]

    if table not in TABLES:
        raise ValueError(f"Unsupported source table: {table}")

    drinks = hook.get_records(f"""
        SELECT distinct country
        FROM {table}
    """)

    aliases = hook.get_records("""
        SELECT alias_country, canonical_country
        FROM staging.country_alias
    """)

    countries = hook.get_records("""
        SELECT name_short
        FROM staging.countries
    """)

    alias_map = {
        alias_country: canonical_country
        for alias_country, canonical_country in aliases
    }

    records = []

    for country in drinks:
        canonical_country = alias_map.get(country, country)

        records.append(
            (
                canonical_country
            )
        )

    # Push data to XCom
    context["ti"].xcom_push(
        key="country_data",
        value=records
    )


def task2(**context):
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="country_data"
    )

    hook = MySqlHook(mysql_conn_id="mysql")

    insert_sql = """
        INSERT INTO dw.dim_geo 
            (
                geo_name
            ) 
            VALUES 
            (
                %s
            )
            ON DUPLICATE KEY UPDATE
            geo_name = VALUES(geo_name)
    """

    for row in records:
        hook.run(insert_sql, parameters=row)

with DAG(
    dag_id='dw_country_alias',
    default_args=default_args,
    start_date=datetime(2026, 1, 20),
    params={
            "table": Param(
                TABLES[0],
                enum=TABLES,
                description="table with country column",
            )
        },
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
