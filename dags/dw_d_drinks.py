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
            c.iso2,
            COALESCE(ca.canonical_country, d.country) AS country,
            c.iso2,
            d.continent        
        FROM staging.drinks d
        LEFT JOIN staging.country_alias ca
            ON d.country = ca.alias_country
        left join staging.countries c
            on COALESCE(ca.canonical_country, d.country) = c.name_short
        where c.iso2 is not null
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
        INSERT INTO dw.dim_geo 
            (
                geo_code, 
                geo_name, 
                geo_type,
                iso2,
                continent_code
            ) 
            VALUES 
            (
                %s,
                %s,
                'country',
                %s,
                %s
            )
            ON DUPLICATE KEY UPDATE
            geo_name = VALUES(geo_name),
            iso2 = VALUES(iso2),
            continent_code = VALUES(continent_code)
    """

    for row in records:
        hook.run(insert_sql, parameters=row)

with DAG(
    dag_id='dw_d_drinks',
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