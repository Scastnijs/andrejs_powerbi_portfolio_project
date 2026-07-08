from datetime import datetime, timedelta
from numbers import Number

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
                wc.population AS value,
                'city_pop' AS indicator_code,
                'Number' AS unit_name,
                DATE_PART('year', CURRENT_DATE) AS year,
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
              AND wc.population IS NOT NULL
              AND c.iso2 IS NOT NULL
        )
        SELECT
            iso2,
            city,
            city_ascii,
            lat,
            lng,
            value,
            indicator_code,
            unit_name,
            year
        FROM cities_deduplicated
        WHERE rn = 1
    """)

    # Push data to XCom
    context["ti"].xcom_push(
        key="city_observation_data",
        value=records
    )


def task2(**context):
    """Processes all records from task1 and performs UPSERT into fact_observation_city."""

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
        key="city_observation_data"
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
                iso2 = None
                city_name = None
                indicator_code = None

                try:
                    iso2 = row[0]
                    city_name = row[1]
                    city_ascii_name = row[2]
                    lat = row[3]
                    lng = row[4]
                    value = row[5] if row[5] is not None else None
                    indicator_code = row[6]
                    unit_name = row[7]
                    year = row[8]

                    # --- Dimension Lookups ---
                    cur.execute("""
                        SELECT geo_key
                        FROM dw.dim_geo
                        WHERE geo_type = 'country'
                          AND (
                              iso2 = %s
                              OR geo_code = %s
                          )
                        LIMIT 1;
                    """, (iso2, iso2))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"No dimension geo record found for ISO2: {iso2}")
                    geo_key = result[0]

                    cur.execute("""
                        SELECT city_key
                        FROM dw.dim_city
                        WHERE geo_key = %s
                          AND city_name = %s
                          AND city_ascii_name <=> %s
                          AND lat <=> %s
                          AND lng <=> %s
                        LIMIT 1;
                    """, (geo_key, city_name, city_ascii_name, lat, lng))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(
                            f"No dimension city record found for city: {city_name}, ISO2: {iso2}"
                        )
                    city_key = result[0]

                    cur.execute("""
                        SELECT unit_key
                        FROM dw.dim_unit
                        WHERE unit_name = %s
                        LIMIT 1;
                    """, (unit_name,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"Could not find unit key for unit name: {unit_name}")
                    unit_key = result[0]

                    cur.execute("""
                        SELECT indicator_key
                        FROM dw.dim_indicator
                        WHERE indicator_code = %s
                        LIMIT 1;
                    """, (indicator_code,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(
                            f"No dimension indicator record found for indicator code: {indicator_code}"
                        )
                    indicator_key = result[0]

                    cur.execute("""
                        SELECT time_key
                        FROM dw.dim_time
                        WHERE year = %s
                        LIMIT 1;
                    """, (year,))
                    result = cur.fetchone()
                    if not result:
                        raise ValueError(f"Could not find time key for year: {year}")
                    time_key = result[0]

                    # --- Build ONE final SQL statement and use it for both logging and execution ---
                    upsert_sql = f"""
INSERT INTO dw.fact_observation_city
    (geo_key, city_key, indicator_key, time_key, unit_key, value, loaded_at)
VALUES ({geo_key}, {city_key}, {indicator_key}, {time_key}, {unit_key}, {sql_literal(value)}, current_timestamp)
ON DUPLICATE KEY UPDATE
    value = VALUES(value),
    loaded_at = VALUES(loaded_at);
""".strip()

                    print(upsert_sql)
                    print()

                    # Execute exactly the same UPSERT SQL that was printed in the log.
                    cur.execute(upsert_sql)
                    processed_count += 1

                except Exception as e:
                    failed_rows.append((i, iso2, city_name, indicator_code, str(e)))
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
        print(
            f"SUCCESSFULLY PROCESSED ALL {processed_count} ROWS "
            "into dw.fact_observation_city and logged all necessary SQL statements."
        )


with DAG(
    dag_id='dw_f_observation_city',
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
