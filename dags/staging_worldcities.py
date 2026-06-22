from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="staging_worldcities",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def read_csv():
        import csv

        csv_file = "/opt/airflow/data/source_csv/worldcities.csv"

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows.append(
                    (
                        row["city"],
                        row["city_ascii"],
                        float(row["lat"]),
                        float(row["lng"]),
                        row["country"],
                        row["iso2"],
                        row["iso3"],
                        row["admin_name"],
                        row["capital"],
                        int(row["population"])
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.worldcities
        (city, city_ascii, lat, lng, country, iso2, iso3, admin_name, capital, population)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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