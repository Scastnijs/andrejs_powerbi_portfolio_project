from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="staging_happiness",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def read_csv():
        import csv

        csv_file = "/opt/airflow/data/source_csv/world_happiness_report/2015.csv"

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                rows.append(
                    (
                        row["Country"],
                        row["Region"],
                        int(row["Happiness Rank"]),
                        float(row["Happiness Score"]),
                        float(row["Standard Error"]),
                        float(row["GDP per Capita"]),
                        float(row["Family"]),
                        float(row["Health"]),
                        float(row["Freedom"]),
                        float(row["Trust"]),
                        float(row["Generosity"]),
                        float(row["Dystopia Residual"])
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.happiness
        (year, country, region, rank, score, serr, gdp_per_capita, family, health, freedom, trust, generosity, dystopia)
        VALUES (2015, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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