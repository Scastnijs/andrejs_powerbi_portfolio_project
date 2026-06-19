from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="staging_drinks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def read_csv():
        import csv

        csv_file = "/opt/airflow/data/source_csv/drinks.csv"

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows.append(
                    (
                        row["country"],
                        int(row["beer_servings"]),
                        int(row["spirit_servings"]),
                        int(row["wine_servings"]),
                        float(row["total_litres_of_pure_alcohol"]),
                        row["continent"]
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.drinks
        (country, beer, spirit, wine, total_litres, continent)
        VALUES (%s, %s, %s, %s, %s, %s)
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