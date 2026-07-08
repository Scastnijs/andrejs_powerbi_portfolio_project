from datetime import datetime, timedelta
from numbers import Number
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
            COALESCE(ca.canonical_country, uh.country) AS country,
            value,
            indicator_code,
            unit_code,
            year
        from
            (select 
                country,
                rank as value,
                'h_rank' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                score as value,
                'h_score' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                gdp_per_capita as value,
                'gdp_pc' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                family as value,
                'family' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                health as value,
                'health' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                freedom as value,
                'freedom' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                trust as value,
                'trust' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                generosity as value,
                'generosity' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            UNION
            select 
                country,
                dystopia as value,
                'dystopia' as indicator_code,
                'NR' as unit_code,
                year
            from staging.happiness
            ) as uh
        LEFT JOIN staging.country_alias ca
            ON uh.country = ca.alias_country
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="vehicles_data",
        value=records
    )


def task2(**context):
    """Processes all records from task1 and performs UPSERT into fact_observation_country."""

    def sql_literal(value):
        """Return a SQL literal for values used in the logged and executed UPSERT."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, Number):
            return str(value)

        # Fallback for unexpected string-like values.
        # The current fact value should be numeric, but this keeps the SQL valid if source data changes.
        escaped_value = str(value).replace("\\", "\\\\").replace("'", "''")
        return f"'{escaped_value}'"

    records = context["ti"].xcom_pull(
        task_ids="task1",
        key="vehicles_data"
    )

    if not records:
        print("No records received from task1. Nothing to process.")
        return

    hook = MySqlHook(mysql_conn_id="mysql")
    processed_count = 0
    failed_rows = []

    print("\n===============================================================")
    print("!!! DIAGNOSTIC OUTPUT: Generated SQL Statements to be run manually !!!")
    print("=============================================================\n")

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for i, row in enumerate(records, start=1):
                country = None
                indicator_code = None

                try:
                    country = row[0]
                    value = row[1] if row[1] is not None else None
                    indicator_code = row[2]
                    unit_code = row[3]
                    year = row[4]

                    # --- Dimension Lookups ---
                    cur.execute("""
                        SELECT geo_key FROM dw.dim_geo WHERE geo_name = %s;
                    """, (country,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"No dimension geo record found for country: {country}")
                    geo_key = result[0]

                    cur.execute("""
                        SELECT unit_key FROM dw.dim_unit WHERE unit_code = %s;
                    """, (unit_code,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"Could not find unit key for '{unit_code}'")
                    unit_key = result[0]

                    cur.execute("""
                        SELECT indicator_key FROM dw.dim_indicator WHERE indicator_code = %s;
                    """, (indicator_code,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"No dimension indicator record found for indicator: {indicator_code}")
                    indicator_key = result[0]

                    cur.execute("""
                        SELECT time_key FROM dw.dim_time WHERE year = %s;
                    """, (year,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"Could not find time key for year: {year}")
                    time_key = result[0]

                    # --- Build ONE final SQL statement and use it for both logging and execution ---
                    upsert_sql = f"""
INSERT INTO dw.fact_observation_country
    (geo_key, indicator_key, time_key, unit_key, value, loaded_at)
VALUES ({geo_key}, {indicator_key}, {time_key}, {unit_key}, {sql_literal(value)}, current_timestamp)
ON DUPLICATE KEY UPDATE
    value = VALUES(value),
    loaded_at = VALUES(loaded_at);
""".strip()

                    # Execute exactly the same UPSERT SQL that was printed in the log.
                    cur.execute(upsert_sql)
                    processed_count += 1

                except Exception as e:
                    failed_rows.append((i, country, indicator_code, str(e)))
                    print(f"[ERROR] Failed to process row {i}: {e}")

            # MySQL connections usually do not autocommit inside Airflow hooks.
            # Explicit commit makes Python execution persist the same way as running the logged SQL manually.
            conn.commit()
            print(f"\nCommitted {processed_count} successful UPSERT statements to MySQL.")

    if failed_rows:
        print("\n\n*** DATA INSERTION SUMMARY ***")
        print(f"SUCCESSFULLY PROCESSED ROWS (and logged SQL): {processed_count} out of {len(records)}")
        print(f"FAILED TO PROCESS (SKIPPED/ERROR) ROWS: {len(failed_rows)}.")
    else:
        print("\n\n*** DATA INSERTION SUMMARY ***")
        print(f"SUCCESSFULLY PROCESSED ALL {processed_count} ROWS into dw.fact_observation_country and logged all necessary SQL statements.")

with DAG(
    dag_id='dw_f_country_happiness',
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