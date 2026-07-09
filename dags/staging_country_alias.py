from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="staging_country_alias",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def read_csv():
        import csv

        csv_file = "/opt/airflow/data/country_alias.csv"

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows.append(
                    (
                        row["alias_country"],
                        row["canonical_country"]
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.country_alias
        (alias_country, canonical_country)
        VALUES (%s, %s)
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