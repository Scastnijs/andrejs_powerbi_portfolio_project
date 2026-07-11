from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG, Param, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_DIR = Path("/opt/airflow/data/source_csv/world_happiness_report")

# Map the canonical staging columns to the REAL header names in each CSV file.
# Columns that do not exist in a specific year are omitted and will be loaded as NULL.
SOURCE_COLUMN_MAP_BY_FILE = {
    "2015.csv": {
        "country": "Country",
        "region": "Region",
        "rank": "Happiness Rank",
        "score": "Happiness Score",
        "gdp_per_capita": "Economy (GDP per Capita)",
        "family": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "trust": "Trust (Government Corruption)",
        "generosity": "Generosity",
        "dystopia": "Dystopia Residual",
    },
    "2016.csv": {
        "country": "Country",
        "region": "Region",
        "rank": "Happiness Rank",
        "score": "Happiness Score",
        "gdp_per_capita": "Economy (GDP per Capita)",
        "family": "Family",
        "health": "Health (Life Expectancy)",
        "freedom": "Freedom",
        "trust": "Trust (Government Corruption)",
        "generosity": "Generosity",
        "dystopia": "Dystopia Residual",
    },
    "2017.csv": {
        "country": "Country",
        "rank": "Happiness.Rank",
        "score": "Happiness.Score",
        "gdp_per_capita": "Economy..GDP.per.Capita.",
        "family": "Family",
        "health": "Health..Life.Expectancy.",
        "freedom": "Freedom",
        "trust": "Trust..Government.Corruption.",
        "generosity": "Generosity",
        "dystopia": "Dystopia.Residual",
    },
    "2018.csv": {
        "country": "Country or region",
        "rank": "Overall rank",
        "score": "Score",
        "gdp_per_capita": "GDP per capita",
        "family": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "trust": "Perceptions of corruption",
        "generosity": "Generosity",
    },
    "2019.csv": {
        "country": "Country or region",
        "rank": "Overall rank",
        "score": "Score",
        "gdp_per_capita": "GDP per capita",
        "family": "Social support",
        "health": "Healthy life expectancy",
        "freedom": "Freedom to make life choices",
        "trust": "Perceptions of corruption",
        "generosity": "Generosity",
    },
}

SOURCE_FILES = sorted(
    file.name
    for file in SOURCE_DIR.glob("*.csv")
    if file.name in SOURCE_COLUMN_MAP_BY_FILE
) or sorted(SOURCE_COLUMN_MAP_BY_FILE)


def clean_value(value):
    if value is None:
        return None

    value = value.strip()
    return None if value == "" else value


def to_float(value):
    value = clean_value(value)
    return None if value is None else float(value)


def to_int(value):
    value = clean_value(value)
    return None if value is None else int(value)


def get_value(row, column_map, target_column):
    source_column = column_map.get(target_column)
    return None if source_column is None else row.get(source_column)


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
        column_map = SOURCE_COLUMN_MAP_BY_FILE[source_file]

        rows = []

        # utf-8-sig also safely removes a UTF-8 BOM if a CSV contains one.
        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
            # Let DictReader read the CSV's actual header row.
            reader = csv.DictReader(f)

            actual_headers = set(reader.fieldnames or [])
            required_headers = set(column_map.values())
            missing_headers = sorted(required_headers - actual_headers)

            if missing_headers:
                raise ValueError(
                    f"Unexpected columns in {source_file}. "
                    f"Missing required headers: {missing_headers}. "
                    f"Actual headers: {reader.fieldnames}"
                )

            for row in reader:
                rows.append(
                    (
                        year,
                        clean_value(get_value(row, column_map, "country")),
                        clean_value(get_value(row, column_map, "region")),
                        to_int(get_value(row, column_map, "rank")),
                        to_float(get_value(row, column_map, "score")),
                        to_float(get_value(row, column_map, "gdp_per_capita")),
                        to_float(get_value(row, column_map, "family")),
                        to_float(get_value(row, column_map, "health")),
                        to_float(get_value(row, column_map, "freedom")),
                        to_float(get_value(row, column_map, "trust")),
                        to_float(get_value(row, column_map, "generosity")),
                        to_float(get_value(row, column_map, "dystopia")),
                    )
                )

        return {"year": year, "rows": rows}

    @task
    def load_to_postgres(data):
        hook = PostgresHook(postgres_conn_id="postgres")

        delete_sql = """
        DELETE FROM staging.happiness
        WHERE year = %s
        """

        insert_sql = """
        INSERT INTO staging.happiness
        (year, country, region, rank, score, gdp_per_capita, family, health, freedom, trust, generosity, dystopia)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Replace the selected year's old rows, including previously shifted data.
            cursor.execute(delete_sql, (data["year"],))

            if data["rows"]:
                cursor.executemany(insert_sql, data["rows"])

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    data = read_csv()
    load_to_postgres(data)
