from datetime import datetime, timedelta
import requests
import json
from airflow.sdk import DAG, Param, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator

# --- Configuration ---
TABLES = [
    "staging.drinks",
    "staging.expectancy",
    "staging.worldcities",
    "staging.vehicles_country",
    "staging.happiness",
]
OLLAMA_URL = "http://localhost:11434/api/chat"
MODEL_NAME = "gemma4" # Or whatever model you prefer

default_args = {
    'owner': 'andrejs',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# --- AI Service Wrapper ---

def get_canonical_country(alias: str, hook: PostgresHook) -> tuple[str, bool]:
    """
    Uses the Ollama API to map a raw country alias to its canonical name 
    and check if it needs registration. Returns (canonical_name, is_newly_discovered).
    """
    # Step 1: Check against source of truth before hitting LLM for new discoveries
    # This prevents paying the LLM API cost for known good aliases
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name_short FROM staging.countries WHERE name_short = %s LIMIT 1;
            """, (alias,))
            # If found in staging.countries, it's definitely valid/canonical
            if cur.fetchone():
                return alias, False

            # Check if it exists as an existing alias
            cur.execute("""
                SELECT canonical_country FROM staging.country_alias WHERE alias_country = %s LIMIT 1;
            """, (alias,))
            result = cur.fetchone()
            if result:
                return result[0], False

    finally:
        conn.close()


    # Step 2: Fallback to LLM for full determination and novelty check
    system_prompt = f"""You are an expert data pipeline validator. Your task is to take a raw country alias, which might be grammatically incorrect or informal. 
    1. Determine the single, correct canonical country name (ISO 3166 standard form).
    2. Analyze if this alias is genuinely new and hasn't been seen before in staging data/common knowledge (assume no other context). If it must be registered as a NEW alias, return true for novelty. Otherwise, return false.

    Your response MUST be a single JSON object with exactly three keys: "canonical_name", "is_newly_discovered" (boolean), and "reasoning".
    Example of success response: {{"canonical_name": "Canada", "is_newly_discovered": true, "reasoning": "Found 'Kanada', mapping to 'Canada' which needs registration."}}"""

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {
                "role": "system", 
                "content": system_prompt
            },
            {"role": "user", "content": f"Validate and map the alias: '{alias}'"}
        ]
    }

    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        content = data["message"]["content"]
        
        # Attempt to extract JSON from the content
        match = json.loads(content.strip()) # Basic assumption for single-object output

        if not isinstance(match, dict) or not all(k in match for k in ["canonical_name", "is_newly_discovered"]):
             raise ValueError("LLM response structure was invalid.")

        return (
            match["canonical_name"], 
            match["is_newly_discovered"]
        )

    except requests.exceptions.RequestException as e:
        print(f"--- [ERROR] Ollama API call failed for '{alias}': {e} ---")
        # Fail safely by assuming no changes and the alias is invalid for transformation purposes
        return None, False 
    except json.JSONDecodeError as e:
        print(f"--- [ERROR] Failed to decode JSON from LLM response for '{alias}'. Raw content: {content[:100]}... Error: {e} ---")
        return None, False

# --- ETL Task Logic (Refactored) ---

def task1_discovery_and_map(**context):
    """
    Phase 1 & 2: Discover raw aliases, validate them using AI/DB rules, and map them.
    Manages the state of newly discovered aliases to commit back to staging.country_alias.
    Returns the list of (canonical_name) for data transformation.
    """
    hook = PostgresHook(postgres_conn_id="postgres")
    # Use a single transaction block for discovery/commit phase
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Find all unique raw country aliases from the primary source table
            table = context["params"]["table"]
            cur.execute(f"""
                SELECT DISTINCT country FROM {table};
            """)
            raw_aliases = [row[0] for row in cur.fetchall()]

        # 2. Discovery and Validation Loop
        newly_discovered_alias_list = []
        transformed_records = []
        all_logs = []

        for alias in raw_aliases:
            canonical, is_new = get_canonical_country(alias, hook)
            
            if canonical is None:
                # Skip if AI service failed to provide results for this alias
                continue 
            
            log_entry = f"Alias '{alias}' mapped to '{canonical}'. New discovery status: {is_new}."
            all_logs.append(log_entry)

            # A. Handle Novel Discoveries (must commit back to staging)
            if is_new and canonical not in [l[0] for l in newly_discovered_alias_list]: # Prevent duplicate processing in this run
                print(f"NEW DISCOVERY FOUND: '{alias}' -> '{canonical}'. Preparing to register.")
                # Execute transaction to commit new alias pair (Requires manual review/confirmation step later)
                cur.execute("""
                    INSERT INTO staging.country_alias (alias_country, canonical_country) 
                    VALUES (%s, %s) ON CONFLICT (alias_country) DO NOTHING;
                """, (alias, canonical))
                newly_discovered_alias_list.append((alias, canonical)) # Track locally for reporting

            # B. Build the final data record regardless of whether it was new or existing
            transformed_records.append(canonical)

        # 3. Final Data Transformation Push (XCom payload)
        context["ti"].xcom_push(
            key="country_data",
            value=transformed_records
        )
        
        # 4. Log Results to XCom for DAG reporting/monitoring
        context["ti"].xcom_push("discovery_logs", "\n".join(all_logs))

    return transformed_records


def task2(**context):
    """
    Phase 3: Load the clean, canonical data into the target dimensional model (dw.dim_geo).
    This function must now provide values for all non-nullable columns in dw.dim_geo.
    """
    # The records here are already cleaned and canonicalized by task1
    records = context["ti"].xcom_pull(task_ids="task1", key="country_data")

    hook = MySqlHook(mysql_conn_id="mysql")

    # UPDATED SQL: Providing placeholders for all non-nullable columns (geo_code, geo_type, etc.)
    insert_sql = """
        INSERT INTO dw.dim_geo 
            (geo_code, geo_name, geo_type) 
            VALUES 
            (%s, %s, 'COUNTRY') -- Placeholder: code, name, type
            ON DUPLICATE KEY UPDATE
            geo_name = VALUES(geo_name),
            geo_code = VALUES(geo_code);
    """

    for row in records:
        # We now unpack the canonical country name (row[0]) into multiple parameters.
        # For this POC, we treat the canonical name as both the code and the initial value.
        canonical_name = row[0]
        hook.run(insert_sql, parameters=(canonical_name, canonical_name, 'COUNTRY'))

# DAG Definition remains the same but now calls the refactored task1/task2
with DAG(
    dag_id='dw_country_alias',
    default_args=default_args,
    start_date=datetime(2026, 1, 20),
    params={
            "table": Param(
                TABLES[0],
                enum=TABLES,
                description="staging table with country column to process",
            )
        },
    tags=['etl', 'country_alias']
) as dag:
    # Task 1 now handles discovery, validation, and staging commit
    task1 = PythonOperator(
        task_id="task1",
        python_callable=task1_discovery_and_map,
    )

    # Task 2 uses the cleaned data from task1 to load into the final target dimension
    task2 = PythonOperator(
        task_id="task2",
        python_callable=task2,
    )

    task1 >> task2