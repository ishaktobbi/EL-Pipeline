"""
DAG: pipeline_dbt_transformations
Description: Runs Cosmos dbt models automatically whenever DAG 1 updates the raw BigLake dataset.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.task_group import TaskGroup
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    RenderConfig,
    LoadMode,
    ExecutionConfig,
    ExecutionMode,
)
from cosmos.profiles import GoogleCloudOauthProfileMapping

from alerts import on_failure_slack_alert

# ----------------------------------------------------------------------
# 0. Environment & Configuration Constants
# ----------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION", "europe-west3")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

AIRFLOW_HOST_MANIFEST = "/opt/airflow/dbt_project/target/manifest.json"
AIRFLOW_HOST_DBT_PROJECT = "/opt/airflow/dbt_project"
AIRFLOW_HOST_DBT_EXE = "/home/airflow/dbt_venv/bin/dbt"


# @provide_session
# def ingestion_is_finished(session=None, **context):
#     active = (
#         session.query(DagRun)
#         .filter(
#             DagRun.dag_id == "pipeline_ingestion",
#             DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]),
#         )
#         .count()
#     )
#     return active == 0  # True = safe to proceed, False = skip this trigger


DEFAULT_ARGS = {
    "owner": "ishak_tobbi",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    'execution_timeout': timedelta(hours=1),  # Fails task if it takes longer than 1 hour
    'on_failure_callback': on_failure_slack_alert, # 🟢 Auto-alert on final failure
}

# ----------------------------------------------------------------------
# 1. DAG Definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="pipeline_transformation",
    default_args=DEFAULT_ARGS,
    description="Transforms raw BigLake tables using dbt upon dataset trigger.",
    schedule=None ,#[TAXI_RAW_DATASET],  # 🔑 <--- Event-driven trigger!
    # start_date=datetime(2019, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "cosmos", "transformations"],
    
) as dag:

    # wait_for_ingestions = ShortCircuitOperator(
    #     task_id="wait_until_ingestion_finished",
    #     python_callable=ingestion_is_finished,
    
    #     )

    transform_data = DbtTaskGroup(
        group_id="dbt_transformations",
          
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            dbt_executable_path=AIRFLOW_HOST_DBT_EXE,
            # setup_operator_args={"pool": "dbt_pool"},
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            emit_datasets=False,
            select=["tag:daily"],
            # exclude=["tag:static"],  # 🔑 Skip static lookups on monthly runs
            should_detach_multiple_parents_tests=True,
        ),
        project_config=ProjectConfig(
            dbt_project_path=AIRFLOW_HOST_DBT_PROJECT,
            project_name="dbt_project",
            manifest_path=AIRFLOW_HOST_MANIFEST,
            install_dbt_deps=False,
            # dbt_vars={
            #     "run_date": "{{ logical_date.strftime('%Y-%m-%d') }}",
            #     "run_year": "{{ logical_date.strftime('%Y') }}",
            #     "run_month": "{{ logical_date.strftime('%m') }}",
            # },
        ),
        profile_config=ProfileConfig(
            profile_name="dbt_project",
            target_name="dev",
            profile_mapping=GoogleCloudOauthProfileMapping(
                conn_id="google_cloud_default",
                profile_args={
                    "project": GCP_PROJECT_ID,
                    "dataset": BIGQUERY_DATASET,
                    "location": GCP_REGION,
                },
            ),
        ),
    )

    # trigger_analytics_dags = TriggerDagRunOperator(
    #         task_id="trigger_analytics_dags",
    #         trigger_dag_id=["saved_queries_loader", "update_dashboard"],  # List of DAG IDs to trigger
    #         wait_for_completion=False,          # Fire and forget once DAG 1 finishes
    #         logical_date="{{ logical_date }}",
    # )
    with TaskGroup("trigger_analytics_dags") as trigger_analytics_dags:
        trigger_dag_a = TriggerDagRunOperator(
            task_id="trigger_saved_queries_loader_dag",
            trigger_dag_id="saved_queries_loader",
            wait_for_completion=False,          # Fire and forget once DAG 1 finishes
            logical_date="{{ logical_date }}",
        )

        trigger_dag_b = TriggerDagRunOperator(
            task_id="trigger_update_dashboard_dag",
            trigger_dag_id="update_dashboard",
            wait_for_completion=False,          # Fire and forget once DAG 1 finishes
            logical_date="{{ logical_date }}",
            
        )

    # wait_for_ingestions >> 
    transform_data >> trigger_analytics_dags