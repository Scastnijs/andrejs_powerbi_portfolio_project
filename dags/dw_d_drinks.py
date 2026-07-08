from datetime import datetime, timedelta

from airflow import DAG
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
        WITH drinks_normalized AS (
            SELECT
                COALESCE(ca.canonical_country, d.country) AS country,
                d.continent,
                d.id
            FROM staging.drinks d
            LEFT JOIN staging.country_alias ca
                ON d.country = ca.alias_country
            WHERE d.continent IS NOT NULL
        ),
        drinks_deduplicated AS (
            SELECT
                country,
                continent,
                ROW_NUMBER() OVER (
                    PARTITION BY country
                    ORDER BY id DESC
                ) AS rn
            FROM drinks_normalized
        )
        SELECT
            c.iso2,
            dd.country,
            c.iso2,
            dd.continent
        FROM drinks_deduplicated dd
        LEFT JOIN staging.countries c
            ON dd.country = c.name_short
        WHERE dd.rn = 1
          AND c.iso2 IS NOT NULL
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="drinks_data",
        value=records
    )


def task2(**context):
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="drinks_data"
    ) or []

    hook = MySqlHook(mysql_conn_id="mysql")

    exists_sql = """
        SELECT 1
        FROM dw.dim_geo
        WHERE geo_code = %s
        LIMIT 1
    """

    update_sql = """
        UPDATE dw.dim_geo
        SET
            geo_name = %s,
            iso2 = %s,
            continent_code = %s
        WHERE geo_code = %s
    """

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
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        for geo_code, geo_name, iso2, continent_code in records:
            cursor.execute(exists_sql, (geo_code,))
            exists = cursor.fetchone()

            if exists:
                cursor.execute(
                    update_sql,
                    (geo_name, iso2, continent_code, geo_code)
                )
            else:
                cursor.execute(
                    insert_sql,
                    (geo_code, geo_name, iso2, continent_code)
                )

        conn.commit()
    finally:
        cursor.close()
        conn.close()


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
