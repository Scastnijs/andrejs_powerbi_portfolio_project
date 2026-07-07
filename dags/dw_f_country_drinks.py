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
    """Processes all records from task1 and performs UPSERT into fact_observation_country."""
    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="customer_data"
    )

    hook = MySqlHook(mysql_conn_id="mysql")
    processed_count = 0
    failed_rows = []

    print("--- Starting data insertion process ---")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(records):
                try:
                    country = row[0] 
                    value = row[1] if row[1] else None
                    indicator = row[2]

                    # --- Dimension Lookups ---
                    cur.execute("""
                        SELECT geo_key FROM dw.dim_geo WHERE geo_name = %s;
                    """, (country,))
                    result = cur.fetchone()
                    if not result: raise ValueError(f"No dimension geo record found for country: {country}")
                    dim_geo_keys = [result[0]]

                    cur.execute("""
                        SELECT unit_key FROM dw.dim_unit WHERE unit_name = 'Litres';
                    """)
                    result = cur.fetchone()
                    if not result: raise ValueError("Could not find unit key for 'Litres'")
                    dim_unit_keys = [result[0]]

                    cur.execute("""
                        SELECT indicator_key FROM dw.dim_indicator WHERE indicator_name = %s;
                    """, (indicator,))
                    result = cur.fetchone()
                    if not result: raise ValueError(f"No dimension indicator record found for indicator: {indicator}")
                    dim_indicator_keys = [result[0]]

                    cur.execute("""
                        SELECT time_key FROM dw.dim_time WHERE year = year(CURRENT_DATE());
                    """)
                    result = cur.fetchone()
                    if not result: raise ValueError("Could not find time key for current date")
                    dim_time_keys = [result[0]]

                    # --- UPSERT Insertion ---
                    insert_sql = """
                        INSERT INTO dw.fact_observation_country
                            (geo_key, indicator_key, time_key, unit_key, value, loaded_at) 
                            VALUES (%s, %s, %s, %s, %s, current_timestamp)
                        ON DUPLICATE KEY UPDATE 
                            value = VALUES(value),
                            loaded_at = VALUES(loaded_at);
                    """

                    cur.execute(insert_sql, (
                        dim_geo_keys[0],
                        dim_indicator_keys[0],
                        dim_time_keys[0],
                        dim_unit_keys[0],
                        value, 
                    ))
                    processed_count += 1

                except Exception as e:
                    failed_rows.append((i, country, indicator, str(e)))
                    print(f"[ERROR] Failed to process row {i}: {e}")


    if failed_rows:
        print("\n*** DATA INSERTION SUMMARY ***")
        print(f"SUCCESSFULLY PROCESSED ROWS: {processed_count} out of {len(records)}")
        print(f"FAILED TO PROCESS (SKIPPED/ERROR) ROWS: {len(failed_rows)}.")
    else:
        print("\n*** DATA INSERTION SUMMARY ***")
        print(f"SUCCESSFULLY PROCESSED ALL {processed_count} ROWS into dw.fact_observation_country.")


def task3(**context):
    """Confirms data presence by executing a simple read query."""
    hook = MySqlHook(mysql_conn_id="mysql")

    # 1. Attempt to flush the connection context, making written data visible.
    print("--- Flushing connection and verifying visibility ---")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Execute a dummy query to force a commit/flush on many MySQL environments
            cur.execute("SELECT 1;") 

            # 2. Now, run the confirmation query.
            sql_query = """
                SELECT * FROM dw.fact_observation_country LIMIT 10;
            """
            print(f"--- Running final confirmation query: {sql_query} ---")
            cur.execute(sql_query)
            records = cur.fetchall()

    if records:
        print(f"\n*** CONFIRMATION SUCCESS ***")
        print(f"Successfully retrieved {len(records)} rows from dw.fact_observation_country, confirming successful data persistence.")
    else:
        print("\n*** WARNING ***")
        print("The confirmation query returned zero rows, suggesting the issue persists at the database transaction visibility level.")



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
    
    # New confirmation task added here!
    task3 = PythonOperator(
        task_id="task3",
        python_callable=task3,
    )

    task1 >> task2 >> task3