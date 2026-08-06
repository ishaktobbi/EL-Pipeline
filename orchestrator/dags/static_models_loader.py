"""
DAG: static_models_loader
Description: On-demand DAG to seed/build static dimension and lookup tables (tagged with 'static').
"""

from datetime import timedelta
import os
from pathlib import Path
from airflow import DAG
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
from cosmos.constants import TestIndirectSelection

from alerts import on_failure_slack_alert

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION", "europe-west3")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

AIRFLOW_HOST_MANIFEST = "/opt/airflow/dbt_project/target/manifest.json"
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")
AIRFLOW_HOST_DBT_EXE = "/home/airflow/dbt_venv/bin/dbt"


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
    dag_id="static_models_loader",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["dbt", "static", "dimensions"],
) as dag:

    transform_static_data = DbtTaskGroup(
        group_id="dbt_static_transformations",
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            dbt_executable_path=AIRFLOW_HOST_DBT_EXE,
            test_indirect_selection=TestIndirectSelection.CAUTIOUS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            emit_datasets=False,
            select=["tag:static"],
            exclude=["tag:fct_dependent"],
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

    transform_static_data