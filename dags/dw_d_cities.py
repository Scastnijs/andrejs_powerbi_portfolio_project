from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'andrejs',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def task1(**context):
    hook = PostgresHook(postgres_conn_id="postgres")

    records = hook.get_records("""
        WITH cities_deduplicated AS (
            SELECT
                wc.iso2,
                wc.city,
                wc.city_ascii,
                wc.lat,
                wc.lng,
                wc.capital,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        wc.iso2,
                        wc.city,
                        wc.city_ascii,
                        wc.lat,
                        wc.lng
                    ORDER BY wc.id DESC
                ) AS rn
            FROM staging.worldcities wc
            LEFT JOIN staging.countries c
                ON wc.iso2 = c.iso2
            WHERE wc.iso2 IS NOT NULL
              AND wc.city IS NOT NULL
              AND c.iso2 IS NOT NULL
        )
        SELECT
            iso2,
            city,
            city_ascii,
            lat,
            lng,
            capital
        FROM cities_deduplicated
        WHERE rn = 1
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="cities_data",
        value=records
    )


def task2(**context):
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="cities_data"
    ) or []

    hook = MySqlHook(mysql_conn_id="mysql")

    geo_key_sql = """
        SELECT geo_key
        FROM dw.dim_geo
        WHERE geo_type = 'country'
          AND (
              iso2 = %s
              OR geo_code = %s
          )
        LIMIT 1
    """

    exists_sql = """
        SELECT city_key
        FROM dw.dim_city
        WHERE geo_key = %s
          AND city_name = %s
          AND city_ascii_name <=> %s
          AND lat <=> %s
          AND lng <=> %s
        LIMIT 1
    """

    update_sql = """
        UPDATE dw.dim_city
        SET
            city_ascii_name = %s,
            lat = %s,
            lng = %s,
            capital = %s
        WHERE city_key = %s
    """

    insert_sql = """
        INSERT INTO dw.dim_city
            (
                geo_key,
                city_name,
                city_ascii_name,
                subdivision_code,
                subdivision_name,
                subdivision_type,
                lat,
                lng,
                capital
            )
            VALUES
            (
                %s,
                %s,
                %s,
                NULL,
                NULL,
                NULL,
                %s,
                %s,
                %s
            )
    """

    conn = hook.get_conn()
    cursor = conn.cursor()

    inserted_count = 0
    updated_count = 0
    skipped_count = 0

    try:
        for iso2, city_name, city_ascii_name, lat, lng, capital in records:
            cursor.execute(geo_key_sql, (iso2, iso2))
            geo_row = cursor.fetchone()

            if not geo_row:
                skipped_count += 1
                continue

            geo_key = geo_row[0]

            cursor.execute(
                exists_sql,
                (geo_key, city_name, city_ascii_name, lat, lng)
            )
            city_row = cursor.fetchone()

            if city_row:
                city_key = city_row[0]
                cursor.execute(
                    update_sql,
                    (city_ascii_name, lat, lng, capital, city_key)
                )
                updated_count += 1
            else:
                cursor.execute(
                    insert_sql,
                    (
                        geo_key,
                        city_name,
                        city_ascii_name,
                        lat,
                        lng,
                        capital
                    )
                )
                inserted_count += 1

        conn.commit()

        print(f"Inserted cities: {inserted_count}")
        print(f"Updated cities: {updated_count}")
        print(f"Skipped cities without geo_key: {skipped_count}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id='dw_d_cities',
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
