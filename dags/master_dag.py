"""Master Airflow DAG for running all project DAGs in controlled sections.

Execution flow:
1. Test DAGs run in parallel.
   - If one child DAG fails, only that failed child DAG task is retried.
2. Staging DAGs run in parallel.
   - If one child DAG fails, only that failed child DAG task is retried.
3. Dimension DAGs run in sequence.
   - If one child DAG fails, the whole dimension section task is retried
     from the first dimension DAG.
4. Fact DAGs run in sequence.
   - If one child DAG fails, the whole fact section task is retried
     from the first fact DAG.

The TaskGroup layout follows the section style from example_task_group.py,
while the trigger/wait/rerun behavior keeps the orchestration logic from the
original master_dag.py.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterable
from datetime import timedelta

import pendulum

from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState


# -----------------------------------------------------------------------------
# Child DAG scope
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Runtime settings
# -----------------------------------------------------------------------------

POKE_INTERVAL_SECONDS = int(os.getenv("MASTER_DAG_POKE_INTERVAL_SECONDS", "30"))
PARALLEL_DAG_RESTARTS = int(os.getenv("MASTER_DAG_PARALLEL_RESTARTS", "1"))
SEQUENTIAL_SECTION_RESTARTS = int(os.getenv("MASTER_DAG_SEQUENTIAL_RESTARTS", "1"))

DEFAULT_ARGS = {
    "owner": "andrejs",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

TERMINAL_STATES = {
    DagRunState.SUCCESS,
    DagRunState.FAILED,
}


# -----------------------------------------------------------------------------
# Child DAG trigger/wait helpers
# -----------------------------------------------------------------------------


def _current_utc_run_stamp() -> str:
    """Return a compact UTC timestamp suitable for unique child DAG run IDs."""
    return pendulum.now("UTC").format("YYYYMMDDTHHmmssSSSSSS")


def _build_child_run_id(section_name: str, child_dag_id: str, attempt: int) -> str:
    return (
        f"master__{section_name}__{child_dag_id}__"
        f"attempt_{attempt}__{_current_utc_run_stamp()}"
    )


def _task_attempt_from_context(context: dict) -> int:
    task_instance = context.get("ti")
    if not task_instance:
        return 1
    return int(getattr(task_instance, "try_number", 1))


def _trigger_child_dag(
    child_dag_id: str,
    section_name: str,
    attempt: int,
    parent_dag_run_id: str | None,
) -> DagRun:
    """Trigger one child DAG and attach master DAG metadata in conf."""
    run_id = _build_child_run_id(
        section_name=section_name,
        child_dag_id=child_dag_id,
        attempt=attempt,
    )

    print(f"Triggering child DAG: {child_dag_id}")
    print(f"Child run_id: {run_id}")

    return trigger_dag(
        dag_id=child_dag_id,
        run_id=run_id,
        conf={
            "triggered_by": "master_dag",
            "master_section": section_name,
            "master_attempt": attempt,
            "master_run_id": parent_dag_run_id,
        },
    )


@provide_session
def _refresh_dag_run(dag_run: DagRun, session=None) -> DagRun:
    """Reload DagRun state from the Airflow metadata database."""
    return (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == dag_run.dag_id,
            DagRun.run_id == dag_run.run_id,
        )
        .one()
    )


def _wait_for_child_dag(child_dag_run: DagRun) -> DagRunState:
    """Block until a child DAG reaches SUCCESS or FAILED."""
    while True:
        current_run = _refresh_dag_run(child_dag_run)
        current_state = DagRunState(current_run.state)

        if current_state in TERMINAL_STATES:
            print(
                f"Child DAG completed: {current_run.dag_id}; "
                f"run_id={current_run.run_id}; state={current_state}"
            )
            return current_state

        print(
            f"Waiting for child DAG: {current_run.dag_id}; "
            f"run_id={current_run.run_id}; current_state={current_state}"
        )
        time.sleep(POKE_INTERVAL_SECONDS)


def _trigger_and_wait_for_success(
    child_dag_id: str,
    section_name: str,
    attempt: int,
    parent_dag_run_id: str | None,
) -> None:
    """Trigger one child DAG, wait for completion, and fail unless it succeeds."""
    child_dag_run = _trigger_child_dag(
        child_dag_id=child_dag_id,
        section_name=section_name,
        attempt=attempt,
        parent_dag_run_id=parent_dag_run_id,
    )
    state = _wait_for_child_dag(child_dag_run)

    if state != DagRunState.SUCCESS:
        raise RuntimeError(
            f"Child DAG '{child_dag_id}' failed in section '{section_name}' "
            f"on attempt {attempt}."
        )


# -----------------------------------------------------------------------------
# Section callables
# -----------------------------------------------------------------------------


def _run_parallel_child_dag(child_dag_id: str, section_name: str, **context) -> None:
    """Run one child DAG inside a parallel section.

    Airflow task retries are used here intentionally. Because every child DAG is
    represented by its own task, only the failed child DAG task is retried.
    """
    parent_dag_run_id = context.get("run_id")
    attempt = _task_attempt_from_context(context)

    _trigger_and_wait_for_success(
        child_dag_id=child_dag_id,
        section_name=section_name,
        attempt=attempt,
        parent_dag_run_id=parent_dag_run_id,
    )


def _run_sequential_section(
    section_name: str,
    child_dag_ids: Iterable[str],
    **context,
) -> None:
    """Run child DAGs in sequence.

    This section is represented by one Airflow task. If any child DAG fails,
    this task fails. On Airflow retry, the whole section starts again from the
    first child DAG.
    """
    parent_dag_run_id = context.get("run_id")
    attempt = _task_attempt_from_context(context)

    for child_dag_id in child_dag_ids:
        _trigger_and_wait_for_success(
            child_dag_id=child_dag_id,
            section_name=section_name,
            attempt=attempt,
            parent_dag_run_id=parent_dag_run_id,
        )


# -----------------------------------------------------------------------------
# TaskGroup builders
# -----------------------------------------------------------------------------


def _build_parallel_section(
    group_id: str,
    tooltip: str,
    section_name: str,
    child_dag_ids: Iterable[str],
) -> TaskGroup:
    """Create a TaskGroup where all child DAGs run in parallel."""
    with TaskGroup(group_id=group_id, tooltip=tooltip) as section:
        section_start = EmptyOperator(task_id="start")
        section_end = EmptyOperator(task_id="end")

        run_child_dag_tasks = [
            PythonOperator(
                task_id=f"run_{child_dag_id}",
                python_callable=_run_parallel_child_dag,
                op_kwargs={
                    "section_name": section_name,
                    "child_dag_id": child_dag_id,
                },
                retries=PARALLEL_DAG_RESTARTS,
                retry_delay=timedelta(minutes=1),
            )
            for child_dag_id in child_dag_ids
        ]

        section_start >> run_child_dag_tasks >> section_end

    return section


def _build_sequential_section(
    group_id: str,
    tooltip: str,
    section_name: str,
    child_dag_ids: Iterable[str],
) -> TaskGroup:
    """Create a TaskGroup where child DAGs run sequentially inside one task."""
    with TaskGroup(group_id=group_id, tooltip=tooltip) as section:
        section_start = EmptyOperator(task_id="start")

        run_section = PythonOperator(
            task_id=f"run_{section_name}_dags_in_sequence",
            python_callable=_run_sequential_section,
            op_kwargs={
                "section_name": section_name,
                "child_dag_ids": list(child_dag_ids),
            },
            retries=SEQUENTIAL_SECTION_RESTARTS,
            retry_delay=timedelta(minutes=1),
        )

        section_end = EmptyOperator(task_id="end")

        section_start >> run_section >> section_end

    return section


# -----------------------------------------------------------------------------
# Master DAG definition
# -----------------------------------------------------------------------------

with DAG(
    dag_id="master_dag",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["master", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    test_section = _build_parallel_section(
        group_id="section_1_test_dags",
        tooltip="Connection and service test DAGs run in parallel.",
        section_name="test",
        child_dag_ids=TEST_DAGS,
    )

    staging_section = _build_parallel_section(
        group_id="section_2_staging_dags",
        tooltip="Staging DAGs run in parallel after tests succeed.",
        section_name="staging",
        child_dag_ids=STAGING_DAGS,
    )

    dimension_section = _build_sequential_section(
        group_id="section_3_dimension_dags",
        tooltip="Dimension DAGs run in dependency order.",
        section_name="dw_d",
        child_dag_ids=DIMENSION_DAGS,
    )

    fact_section = _build_sequential_section(
        group_id="section_4_fact_dags",
        tooltip="Fact DAGs run after dimensions are loaded.",
        section_name="dw_f",
        child_dag_ids=FACT_DAGS,
    )

    end = EmptyOperator(task_id="end")

    start >> test_section >> staging_section >> dimension_section >> fact_section >> end
