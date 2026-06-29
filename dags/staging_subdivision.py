from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="staging_subdivisions",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def read_csv():
        import csv

        csv_file = "/opt/airflow/data/source_csv/subdivisions.csv"

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows.append(
                    (
                        row["country_code_alpha2"],
                        row["subdivision_code_iso3166-2"],
                        row["subdivision_name"],
                        row["language_code"],
                        row["parent_subdivision"],
                        row["category"],
                        row["localVariant"]
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.subdivisions
        (iso2, subdivision_code, name, language_code, parent_subdivision, category, local_variant)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        conn = hook.get_conn()
        cursor = conn.cursor()

        for row in rows:
            cursor.execute(insert_sql, row)

        conn.commit()

        cursor.close()
        conn.close()

    data = read_csv()

    load_to_postgres(data)