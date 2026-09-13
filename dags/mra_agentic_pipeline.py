from datetime import timedelta
import sys

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


CODE_DIR = "/opt/airflow/code"
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

# Run every day at 10:00 in the DAG timezone (Asia/Bangkok).
DAG_SCHEDULE = "0 8 * * 1"
LAST_SCHEDULED_RUN = pendulum.datetime(2026, 12, 28, 8, 0, tz="Asia/Bangkok")


DEFAULT_ARGS = {
    "owner": "subhintra",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

ROLE_KEYS = [
    "doctor",
    "nurse",
    "pharmacist",
    "dietitian",
    "physiotherapist",
    "csr",
    "lab",
    "xray",
]


def run_stage(function_name, role_key=None, **context):
    from mra_etl_pipeline import __dict__ as pipeline_functions

    function = pipeline_functions[function_name]
    if role_key is not None:
        return function(role_key=role_key)
    return function()


with DAG(
    dag_id="mra_agentic_pipeline",
    description="Modern MRA ETL with deterministic data quality and AI readiness",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    # Airflow's daily cron uses the start of the data interval as logical_date.
    # Ending one interval earlier makes the final actual run occur on 15 Sep at 10:00.
    end_date=LAST_SCHEDULED_RUN.subtract(days=1),
    schedule=DAG_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    tags=["mra", "etl", "data-quality", "agentic-ai"],
) as dag:
    start = EmptyOperator(task_id="start")

    sync_onedrive = PythonOperator(
        task_id="sync_onedrive_inputs",
        python_callable=run_stage,
        op_kwargs={"function_name": "sync_onedrive_inputs"},
    )

    sync_moi_references = PythonOperator(
        task_id="sync_moi_reference_inputs",
        python_callable=run_stage,
        op_kwargs={"function_name": "sync_moi_reference_inputs"},
    )

    check_files = PythonOperator(
        task_id="check_input_files",
        python_callable=run_stage,
        op_kwargs={"function_name": "check_input_files"},
    )

    extract_tasks = []
    for role_key in ROLE_KEYS:
        extract_task = PythonOperator(
            task_id=f"extract_{role_key}",
            python_callable=run_stage,
            op_kwargs={"function_name": "extract_role", "role_key": role_key},
            pool="default_pool",
        )
        extract_tasks.append(extract_task)

    combine = PythonOperator(
        task_id="combine_fact",
        python_callable=run_stage,
        op_kwargs={"function_name": "combine_fact"},
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=run_stage,
        op_kwargs={"function_name": "validate_data_quality"},
    )

    build_moi_dimensions = PythonOperator(
        task_id="build_moi_reference_dimensions",
        python_callable=run_stage,
        op_kwargs={"function_name": "build_moi_reference_dimensions"},
    )

    join = PythonOperator(
        task_id="join_mraudit_lists",
        python_callable=run_stage,
        op_kwargs={"function_name": "join_mraudit_lists"},
    )

    map_moi = PythonOperator(
        task_id="map_moi_references",
        python_callable=run_stage,
        op_kwargs={"function_name": "map_moi_references"},
    )

    reconcile_ha = PythonOperator(
        task_id="reconcile_ha_report",
        python_callable=run_stage,
        op_kwargs={"function_name": "reconcile_ha_report"},
    )

    compare = PythonOperator(
        task_id="compare_reference",
        python_callable=run_stage,
        op_kwargs={"function_name": "compare_reference"},
    )

    quality_report = PythonOperator(
        task_id="build_quality_report",
        python_callable=run_stage,
        op_kwargs={"function_name": "build_quality_report"},
    )

    gate = PythonOperator(
        task_id="quality_gate",
        python_callable=run_stage,
        op_kwargs={"function_name": "quality_gate"},
    )

    load = PythonOperator(
        task_id="load_outputs",
        python_callable=run_stage,
        op_kwargs={"function_name": "load_outputs"},
    )

    load_mariadb = PythonOperator(
        task_id="load_to_mariadb",
        python_callable=run_stage,
        op_kwargs={"function_name": "load_to_mariadb"},
    )

    agent_payload = PythonOperator(
        task_id="build_agent_payload",
        python_callable=run_stage,
        op_kwargs={"function_name": "build_agent_payload"},
    )

    n8n = PythonOperator(
        task_id="notify_n8n",
        python_callable=run_stage,
        op_kwargs={"function_name": "notify_n8n"},
    )

    human_review = PythonOperator(
        task_id="human_review_ready",
        python_callable=run_stage,
        op_kwargs={"function_name": "human_review_ready"},
    )

    end = EmptyOperator(task_id="end")

    start >> [sync_onedrive, sync_moi_references]
    [sync_onedrive, sync_moi_references] >> check_files
    check_files >> extract_tasks
    check_files >> build_moi_dimensions
    extract_tasks >> combine
    combine >> validate >> join
    [join, build_moi_dimensions] >> map_moi >> reconcile_ha
    join >> compare
    [reconcile_ha, compare] >> quality_report >> gate >> load
    load >> load_mariadb >> agent_payload >> n8n >> human_review >> end
