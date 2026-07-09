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
        WITH countries_deduplicated AS (
            SELECT
                c.iso2,
                c.name_short,
                c.iso3,
                ROW_NUMBER() OVER (
                    PARTITION BY c.iso2
                    ORDER BY c.id DESC
                ) AS rn
            FROM staging.countries c
            WHERE c.iso2 IS NOT NULL
              AND c.name_short IS NOT NULL
        )
        SELECT
            iso2 AS geo_code,
            name_short AS geo_name,
            'country' AS geo_type,
            iso2,
            iso3
        FROM countries_deduplicated
        WHERE rn = 1
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="geo_data",
        value=records
    )


def task2(**context):
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="geo_data"
    ) or []

    hook = MySqlHook(mysql_conn_id="mysql")

    exists_sql = """
        SELECT geo_key
        FROM dw.dim_geo
        WHERE geo_code = %s
        LIMIT 1
    """

    update_sql = """
        UPDATE dw.dim_geo
        SET
            geo_name = %s,
            geo_type = %s,
            iso2 = %s,
            iso3 = %s
        WHERE geo_key = %s
    """

    insert_sql = """
        INSERT INTO dw.dim_geo
            (
                geo_code,
                geo_name,
                geo_type,
                iso2,
                iso3,
                continent_code,
                region,
                is_eu_member_current,
                eu_join_year,
                eu_leave_year
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
            )
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    inserted_count = 0
    updated_count = 0

    try:
        for geo_code, geo_name, geo_type, iso2, iso3 in records:
            cursor.execute(exists_sql, (geo_code,))
            geo_row = cursor.fetchone()

            if geo_row:
                geo_key = geo_row[0]
                cursor.execute(
                    update_sql,
                    (geo_name, geo_type, iso2, iso3, geo_key)
                )
                updated_count += 1
            else:
                cursor.execute(
                    insert_sql,
                    (
                        geo_code,
                        geo_name,
                        geo_type,
                        iso2,
                        iso3
                    )
                )
                inserted_count += 1

        conn.commit()

        print(f"Inserted geo records: {inserted_count}")
        print(f"Updated geo records: {updated_count}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id='dw_d_geo',
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
