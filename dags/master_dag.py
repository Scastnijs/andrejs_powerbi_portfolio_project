"""Master Airflow DAG for running all project DAGs in controlled sections.

Execution flow:
1. Test DAGs run in parallel.
   - If one child DAG fails, only that failed child DAG task is retried.
2. Staging DAGs run in parallel.
   - If one child DAG fails, only that failed child DAG task is retried.
3. Dimension DAGs run in sequence.
   - Every child DAG is visible as its own task.
   - If one child DAG fails, only that child DAG task is retried.
4. Fact DAGs run in sequence.
   - Every child DAG is visible as its own task.
   - If one child DAG fails, only that child DAG task is retried.

Airflow 3 note:
This DAG intentionally uses TriggerDagRunOperator instead of custom Python code
that calls trigger_dag(), provide_session, or session.query(DagRun). Direct ORM
metadata database access from task runtime code is not allowed in Airflow 3.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import timedelta

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, TaskGroup
from airflow.utils.state import DagRunState


# -----------------------------------------------------------------------------
# Child DAG scope
# -----------------------------------------------------------------------------

TEST_DAGS = [
    "mysql_test",
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
SEQUENTIAL_DAG_RESTARTS = int(os.getenv("MASTER_DAG_SEQUENTIAL_RESTARTS", "1"))

DEFAULT_ARGS = {
    "owner": "andrejs",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# -----------------------------------------------------------------------------
# Child DAG task factory
# -----------------------------------------------------------------------------


def _make_child_dag_task(
    child_dag_id: str,
    section_name: str,
    retries: int,
) -> TriggerDagRunOperator:
    """Create one Airflow task that triggers and waits for one child DAG.

    The trigger_run_id includes the parent DAG timestamp and task try number.
    This makes retry runs unique, so a failed child DAG can be triggered again
    by the retried master task without colliding with the previous child run_id.
    """
    return TriggerDagRunOperator(
        task_id=f"run_{child_dag_id}",
        trigger_dag_id=child_dag_id,
        trigger_run_id=(
            f"master__{section_name}__{child_dag_id}__"
            "{{ ts_nodash }}__try_{{ ti.try_number }}"
        ),
        conf={
            "triggered_by": "master_dag",
            "master_section": section_name,
            "master_run_id": "{{ run_id }}",
            "master_task_id": "{{ task.task_id }}",
            "master_try_number": "{{ ti.try_number }}",
        },
        wait_for_completion=True,
        poke_interval=POKE_INTERVAL_SECONDS,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        retries=retries,
        retry_delay=timedelta(minutes=1),
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
    """Create a TaskGroup where all child DAG tasks run in parallel."""
    with TaskGroup(group_id=group_id, tooltip=tooltip) as section:
        section_start = EmptyOperator(task_id="start")
        section_end = EmptyOperator(task_id="end")

        child_dag_tasks = [
            _make_child_dag_task(
                child_dag_id=child_dag_id,
                section_name=section_name,
                retries=PARALLEL_DAG_RESTARTS,
            )
            for child_dag_id in child_dag_ids
        ]

        section_start >> child_dag_tasks >> section_end

    return section


def _build_sequential_section(
    group_id: str,
    tooltip: str,
    section_name: str,
    child_dag_ids: Iterable[str],
) -> TaskGroup:
    """Create a TaskGroup where every child DAG is its own sequential task."""
    with TaskGroup(group_id=group_id, tooltip=tooltip) as section:
        section_start = EmptyOperator(task_id="start")
        section_end = EmptyOperator(task_id="end")

        previous_task = section_start

        for child_dag_id in child_dag_ids:
            child_dag_task = _make_child_dag_task(
                child_dag_id=child_dag_id,
                section_name=section_name,
                retries=SEQUENTIAL_DAG_RESTARTS,
            )
            previous_task >> child_dag_task
            previous_task = child_dag_task

        previous_task >> section_end

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
        tooltip="Dimension DAGs run in dependency order, with one task per DAG.",
        section_name="dw_d",
        child_dag_ids=DIMENSION_DAGS,
    )

    fact_section = _build_sequential_section(
        group_id="section_4_fact_dags",
        tooltip="Fact DAGs run in dependency order, with one task per DAG.",
        section_name="dw_f",
        child_dag_ids=FACT_DAGS,
    )

    end = EmptyOperator(task_id="end")

    start >> test_section >> staging_section >> dimension_section >> fact_section >> end
