from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_DIR = Path("/opt/airflow/data/source_csv/world_happiness_report")
INSERT_COLUMNS = [
    "year",
    "country",
    "region",
    "rank",
    "score",
    "gdp_per_capita",
    "family",
    "health",
    "freedom",
    "trust",
    "generosity",
    "dystopia",
]

CSV_COLUMNS_BY_FILE = {
    "2015.csv": INSERT_COLUMNS[1:],
    "2016.csv": [
        "country",
        "region",
        "rank",
        "score",
        "gdp_per_capita",
        "family",
        "health",
        "freedom",
        "trust",
        "generosity",
        "dystopia",
    ],
    "2017.csv": [
        "country",
        "rank",
        "score",
        "gdp_per_capita",
        "family",
        "health",
        "freedom",
        "generosity",
        "trust",
        "dystopia",
    ],
    "2018.csv": [
        "rank",
        "country",
        "score",
        "gdp_per_capita",
        "family",
        "health",
        "freedom",
        "generosity",
        "trust",
    ],
    "2019.csv": [
        "rank",
        "country",
        "score",
        "gdp_per_capita",
        "family",
        "health",
        "freedom",
        "generosity",
        "trust",
    ],
}
SOURCE_FILES = sorted(
    file.name for file in SOURCE_DIR.glob("*.csv") if file.name in CSV_COLUMNS_BY_FILE
) or sorted(CSV_COLUMNS_BY_FILE)


def to_float(value):
    return None if value in (None, "") else float(value)


def to_int(value):
    return None if value in (None, "") else int(value)


with DAG(
    dag_id="staging_happiness",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "source_file": Param(
            SOURCE_FILES[0],
            enum=SOURCE_FILES,
            description="CSV file from data/source_csv/world_happiness_report",
        )
    },
) as dag:

    @task
    def read_csv():
        import csv

        context = get_current_context()
        source_file = context["params"]["source_file"]
        csv_file = SOURCE_DIR / source_file
        year = int(Path(source_file).stem)
        csv_columns = CSV_COLUMNS_BY_FILE[source_file]

        rows = []

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=csv_columns)
            next(reader)

            for row in reader:
                rows.append(
                    (
                        year,
                        row.get("country"),
                        row.get("region"),
                        to_int(row.get("rank")),
                        to_float(row.get("score")),
                        to_float(row.get("gdp_per_capita")),
                        to_float(row.get("family")),
                        to_float(row.get("health")),
                        to_float(row.get("freedom")),
                        to_float(row.get("trust")),
                        to_float(row.get("generosity")),
                        to_float(row.get("dystopia")),
                    )
                )

        return rows

    @task
    def load_to_postgres(rows):
        hook = PostgresHook(postgres_conn_id="postgres")

        insert_sql = """
        INSERT INTO staging.happiness
        (year, country, region, rank, score, gdp_per_capita, family, health, freedom, trust, generosity, dystopia)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
