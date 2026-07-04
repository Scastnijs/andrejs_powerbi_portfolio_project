from datetime import datetime, timedelta
import json
# Core Airflow Hooks for connectivity
from airflow.providers.http.hooks.http import HttpHook 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.python import PythonOperator

# Standard Airflow components
from airflow.sdk import DAG, Param 

# --- Configuration ---
TABLES = [
    "staging.drinks",
    "staging.expectancy",
    "staging.worldcities",
    "staging.vehicles_country",
    "staging.happiness",
]

OLLAMA_CONN_ID = "Ollama" # REQUIRED: Must match an existing Airflow Connection ID pointing to localhost:11434

default_args = {
    'owner': 'andrejs',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# --- The AI Service Wrapper Function ---

def get_canonical_country(alias: str, hook: PostgresHook, model_name: str) -> tuple[str | None, bool]:
    """
    Uses the Ollama API via HttpHook to map a raw country alias. 
    Returns (canonical_name, is_newly_discovered).
    """
    # Step 1: Check against source of truth first (DB local checks are fast and reliable)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name_short FROM staging.countries WHERE name_short = %s LIMIT 1;
            """, (alias,))
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

     # Step 2: Fallback to LLM via HttpHook for full determination and novelty check
    try:
        http_hook = HttpHook(method='POST', http_conn_id=OLLAMA_CONN_ID)
    except Exception as e:
        print(f"--- [FATAL] Could not initialize HttpHook. Check Airflow Connection ID '{OLLAMA_CONN_ID}'. Error: {e} ---")
        return None, False

    system_prompt = f"""You are an expert data pipeline validator. Your task is to take a raw country alias, which might be grammatically incorrect or informal. 
    1. Determine the single, correct canonical country name (ISO 3166 standard form).
    2. Analyze if this alias is genuinely new and hasn't been seen before in staging data/common knowledge (assume no other context). If it must be registered as a NEW alias, return true for novelty. Otherwise, return false.

    Your response MUST be a single JSON object with exactly three keys: "canonical_name", "is_newly_discovered" (boolean), and "reasoning".
    Example of success response: {{"canonical_name": "Canada", "is_newly_discovered": true, "reasoning": "Found 'Kanada', mapping to 'Canada' which needs registration."}}"""

    payload = json.dumps({
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Validate and map the alias: '{alias}'"}
        ],
        "stream": False
    })

    try:
       # Run the HTTP call using HttpHook. 
        resp = http_hook.run(
            endpoint='/api/chat',
            extra_options={"headers": {"Content-Type": "application/json"}},
            data=payload,
            headers={"Accept": "application/json"}
        )

        print(resp.status_code)
        print(resp.text)

        response_json = resp.json()
        content = response_json.get("message", {}).get("content", "")

        content = (
            content
            .replace("```json", "")
            .replace("```", "")
            .strip()
        )

        match = json.loads(content.strip()) 

        if not isinstance(match, dict) or not all(k in match for k in ["canonical_name", "is_newly_discovered"]):
             raise ValueError("LLM response structure was invalid and missing required keys.")

        return (
            match["canonical_name"], 
            match["is_newly_discovered"]
        )

    except Exception as e:
        print(f"--- [ERROR] LLM call failed for '{alias}' via HttpHook: {type(e).__name__}: {e} ---")
        return None, False

# --- ETL Task Logic ---

def task1_discovery_and_map(**context):
    """
    Phase 1 & 2: Discover raw aliases, validate them using AI/DB rules, and map them.
    Commits new discoveries back to staging.country_alias via a single transaction.
    Pushes the cleaned data to XCom for transformation by task2.
    """
    hook = PostgresHook(postgres_conn_id="postgres")
    model_name = context['params']['llm_model'] 

    # Use a single transactional block for discovery and commit phase
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Find all unique raw country aliases from the primary source table
            table = context["params"]["source_table"]
            cur.execute(f"""
                SELECT DISTINCT country FROM {table};
            """)
            raw_aliases = [row[0] for row in cur.fetchall()]

        # 2. Discovery and Validation Loop
        newly_discovered_alias_list = [] # (alias, canonical)
        transformed_records = []
        all_logs = []

        for alias in raw_aliases:
            canonical, is_new = get_canonical_country(alias, hook, model_name)
            
            if canonical is None:
                continue 
            
            log_entry = f"Alias '{alias}' mapped to '{canonical}'. New discovery status: {is_new}."
            all_logs.append(log_entry)

            # A. Handle Novel Discoveries (The database transactional commit step)
            if is_new and canonical not in [l[0] for l in newly_discovered_alias_list]: 
                print(f"NEW DISCOVERY FOUND: '{alias}' -> '{canonical}'. Preparing to register.")
                cur.execute("""
                    INSERT INTO staging.country_alias (alias_country, canonical_country) 
                    VALUES (%s, %s) ON CONFLICT (alias_country) DO NOTHING;
                """, (alias, canonical))
                newly_discovered_alias_list.append((alias, canonical)) # Track locally for reporting

            # B. Build the final data record
            transformed_records.append(canonical)

          # 3. Final Data Transformation Push (XCom payload)
        context["ti"].xcom_push("country_data", transformed_records)
        
        # 4. Log Results to XCom for DAG reporting/monitoring
        context["ti"].xcom_push("discovery_logs", "\n".join(all_logs))

    return transformed_records

# --- ETL Task 2 (Loading) ---

def task2(**context):
    """
    Phase 3: Load the clean, canonical data into the target dimensional model (dw.dim_geo).
    """
    # The records here are already cleaned and canonicalized by task1
    records = context["ti"].xcom_pull(task_ids="task1", key="country_data")

    hook = MySqlHook(mysql_conn_id="mysql")

    # SQL parameters: (geo_code, geo_name, geo_type)
    insert_sql = """
        INSERT INTO dw.dim_geo 
            (geo_code, geo_name, geo_type) 
            VALUES 
            (%s, %s, %s) 
            ON DUPLICATE KEY UPDATE
            geo_name = VALUES(geo_name),
            geo_code = VALUES(geo_code);
    """

    for canonical_name in records:
        hook.run(
            insert_sql,
            parameters=(canonical_name, canonical_name, "COUNTRY")
        )

# DAG Definition 
with DAG(
    dag_id='dw_country_alias',
    default_args=default_args,
    start_date=datetime(2026, 1, 20),
    params={
            "source_table": Param(
                TABLES[0],
                enum=TABLES,
                description="staging table with country column to process (e.g., staging.drinks)",
            ),
            "llm_model": Param("gemma4", description="The model name to use for Ollama calls.") 
        },
    tags=['etl', 'country_alias']
) as dag:
    # Task 1 now handles discovery, validation, and staging commit (the core logic)
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