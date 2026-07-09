"""Master Airflow DAG for running the project DAGs in controlled sections.

The DAG executes four sections in order:
1. Connection/test DAGs ending in ``_test`` in parallel.
2. Staging DAGs starting with ``staging_`` in parallel.
3. Dimension warehouse DAGs starting with ``dw_d_`` in sequence.
4. Fact warehouse DAGs starting with ``dw_f_`` in sequence.

Parallel sections restart only failed child DAGs inside the same section attempt.
Sequential sections are represented as one Airflow task with retries, so a child DAG
failure restarts the whole section from the first DAG on the next task retry.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Iterable

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

TEST_DAGS = [
    "mysql_test",
    "ollama_test",
    "oracle_test",
    "postgres_test",
]

STAGING_DAGS = [
    "staging_countries",
    "staging_country_alias",
    "staging_drinks",
    "staging_expectancy",
    "staging_happiness",
    "staging_subdivisions",
    "staging_vehicles",
    "staging_worldcities",
]

DIMENSION_DAGS = [
    "dw_d_geo",
    "dw_d_cities",
    "dw_d_drinks",
    "dw_d_happiness",
]

FACT_DAGS = [
    "dw_f_country_drinks",
    "dw_f_country_happiness",
    "dw_f_country_vehicles",
    "dw_f_observation_city",
]

POKE_INTERVAL_SECONDS = int(os.getenv("MASTER_DAG_POKE_INTERVAL_SECONDS", "30"))
PARALLEL_DAG_RESTARTS = int(os.getenv("MASTER_DAG_PARALLEL_RESTARTS", "1"))

DEFAULT_ARGS = {
    "owner": "andrejs",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

TERMINAL_STATES = {
    DagRunState.SUCCESS,
    DagRunState.FAILED,
}


def _run_id(section_name: str, dag_id: str, attempt: int) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    return f"master__{section_name}__{dag_id}__attempt_{attempt}__{timestamp}"


def _trigger_child_dag(dag_id: str, section_name: str, attempt: int) -> DagRun:
    return trigger_dag(
        dag_id=dag_id,
        run_id=_run_id(section_name, dag_id, attempt),
        conf={
            "triggered_by": "master_dag",
            "master_section": section_name,
            "section_attempt": attempt,
        },
    )


@provide_session
def _refresh_dag_run(dag_run: DagRun, session=None) -> DagRun:
    return session.query(DagRun).filter(DagRun.id == dag_run.id).one()


def _wait_for_dag_run(dag_run: DagRun) -> DagRunState:
    while True:
        current_run = _refresh_dag_run(dag_run)
        if current_run.state in TERMINAL_STATES:
            return DagRunState(current_run.state)
        time.sleep(POKE_INTERVAL_SECONDS)


def _run_parallel_section(section_name: str, dag_ids: Iterable[str], **_context) -> None:
    """Run DAGs in parallel and restart only the child DAGs that fail."""
    pending_dag_ids = list(dag_ids)

    for attempt in range(1, PARALLEL_DAG_RESTARTS + 2):
        dag_runs = {
            dag_id: _trigger_child_dag(dag_id, section_name, attempt)
            for dag_id in pending_dag_ids
        }
        failed_dag_ids = []

        while dag_runs:
            for dag_id, dag_run in list(dag_runs.items()):
                current_run = _refresh_dag_run(dag_run)
                if current_run.state == DagRunState.SUCCESS:
                    dag_runs.pop(dag_id)
                elif current_run.state == DagRunState.FAILED:
                    failed_dag_ids.append(dag_id)
                    dag_runs.pop(dag_id)
            if dag_runs:
                time.sleep(POKE_INTERVAL_SECONDS)

        if not failed_dag_ids:
            return

        pending_dag_ids = failed_dag_ids

    raise RuntimeError(
        f"Section '{section_name}' failed after restarting child DAGs: "
        f"{', '.join(pending_dag_ids)}"
    )


def _run_sequential_section(section_name: str, dag_ids: Iterable[str], **_context) -> None:
    """Run DAGs in sequence; task retries restart the entire section."""
    for child_dag_id in dag_ids:
        dag_run = _trigger_child_dag(child_dag_id, section_name, attempt=1)
        state = _wait_for_dag_run(dag_run)
        if state != DagRunState.SUCCESS:
            raise RuntimeError(
                f"Child DAG '{child_dag_id}' failed in sequential section "
                f"'{section_name}'. The whole section will restart on task retry."
            )


with DAG(
    dag_id="master_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["master", "orchestration"],
) as dag:
    run_tests = PythonOperator(
        task_id="section_1_test_dags",
        python_callable=_run_parallel_section,
        op_kwargs={"section_name": "test", "dag_ids": TEST_DAGS},
    )

    run_staging = PythonOperator(
        task_id="section_2_staging_dags",
        python_callable=_run_parallel_section,
        op_kwargs={"section_name": "staging", "dag_ids": STAGING_DAGS},
    )

    run_dimensions = PythonOperator(
        task_id="section_3_dw_d_dags",
        python_callable=_run_sequential_section,
        op_kwargs={"section_name": "dw_d", "dag_ids": DIMENSION_DAGS},
    )

    run_facts = PythonOperator(
        task_id="section_4_dw_f_dags",
        python_callable=_run_sequential_section,
        op_kwargs={"section_name": "dw_f", "dag_ids": FACT_DAGS},
    )

    run_tests >> run_staging >> run_dimensions >> run_facts
