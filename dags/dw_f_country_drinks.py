from datetime import datetime, timedelta
from airflow import DAG, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
        select 
            COALESCE(ca.canonical_country, ud.country) AS country,
            value,
            indicator
        from
            (select 
                country,
                beer as value,
                'beer' as indicator
            from staging.drinks
            UNION
            select 
                country,
                spirit as value,
                'spirit' as indicator 
            from staging.drinks
            UNION
            select 
                country,
                wine as value,
                'wine' as indicator
            from staging.drinks
            UNION
            select
                country,
                total_litres as value,
                'total_litres' as indicator
            from staging.drinks
            ) as ud
        LEFT JOIN staging.country_alias ca
            ON ud.country = ca.alias_country
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

    for row in records:
        country = row[0]  # Assume tuple structure: (country, value, indicator)
        value = row[1]
        indicator = row[2]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    # Get dw.dim_geo key for the country in this record (row)
                    cur.execute(f"""
                        SELECT geo_key
                        FROM dw.dim_geo
                        WHERE geo_name = %s;
                    """, (country,)) # Use parameterized query

                    dim_geo_keys = [row[0] for row in cur.fetchall()]

                    # Get dw.dim_unit key for Litres
                    cur.execute("""
                        SELECT unit_key FROM dw.dim_unit WHERE unit_name = 'Litres';
                    """)
                    dim_unit_keys = [row[0] for row in cur.fetchall()]

                    # Get dw.dim_indicator key for the indicator in this record (row)
                    cur.execute(f"""
                        SELECT indicator_key
                        FROM dw.dim_indicator
                        WHERE indicator_name = %s;
                    """, (indicator,)) # Use parameterized query

                    dim_indicator_keys = [row[0] for row in cur.fetchall()]

                    # Get dw.dim_time key for current date
                    cur.execute("""
                        SELECT time_key FROM dw.dim_time WHERE year = year(CURRENT_DATE());
                    """)
                    dim_time_keys = [row[0] for row in cur.fetchall()]

                    # Insert observation using the retrieved keys and values from the current row
                    insert_sql = """
                        INSERT INTO dw.fact_observation_country
                            (
                                geo_key, 
                                indicator_key, 
                                time_key,
                                unit_key,
                                value,
                                loaded_at
                            ) 
                            VALUES 
                            (%s, %s, %s, %s, %s, current_timestamp);
                    """

                    cur.execute(insert_sql, (
                        dim_geo_keys[0],
                        dim_indicator_keys[0],
                        dim_time_keys[0],
                        dim_unit_keys[0],
                        value,
                    ))

                except Exception as e:
                    # Handle cases where key lookups fail (e.g., data mismatch)
                    print(f"Failed to process row ({country}, {value}, {indicator}): {e}")
with DAG(
    dag_id='dw_f_country_drinks',
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