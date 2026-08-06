"""
DAG: saved_queries_loader
Description: On-demand or scheduled execution for dbt analytical exports and saved query models (tagged with 'saved_query').
"""

from datetime import timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.operators.python import ShortCircuitOperator
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

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION", "europe-west3")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

AIRFLOW_HOST_MANIFEST = "/opt/airflow/dbt_project/target/manifest.json"
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")
AIRFLOW_HOST_DBT_EXE = "/home/airflow/dbt_venv/bin/dbt"



@provide_session
def transformation_is_finished(session=None, **context):
    active = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "pipeline_transformation",
            DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]),
        )
        .count()
    )
    return active == 0  # True = safe to proceed, False = skip this trigger

DEFAULT_ARGS = {
    "owner": "ishak_tobbi",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    'execution_timeout': timedelta(minutes=20),  # Fails task if it takes longer than 1 hour
    'on_failure_callback': on_failure_slack_alert, # 🟢 Auto-alert on final failure

}

with DAG(
    dag_id="saved_queries_loader",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["dbt", "saved_queries", "analytics"],
) as dag:

    wait_for_transformation = ShortCircuitOperator(
        task_id="wait_until_transformation_finished",
        python_callable=transformation_is_finished,

    )

    transform_saved_queries = DbtTaskGroup(
        group_id="dbt_saved_query_transformations",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            dbt_executable_path=AIRFLOW_HOST_DBT_EXE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            emit_datasets=False,
            select=["tag:saved_query"],
        ),
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            project_name="dbt_project",
            manifest_path=AIRFLOW_HOST_MANIFEST,
            install_dbt_deps=False,
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

    wait_for_transformation >> transform_saved_queries