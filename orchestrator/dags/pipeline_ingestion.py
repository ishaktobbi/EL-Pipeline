"""
DAG: pipeline_ingestion
Description: Ingests raw taxi CSVs to GCS via Docker, exposes Parquet via BigLake External Tables,
             refreshes BigQuery metadata cache, and emits a Dataset event upon completion.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from alerts import on_failure_slack_alert, on_sla_miss_slack_alert

# ----------------------------------------------------------------------
# 0. Environment & Configuration Constants
# ----------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
BQ_CONNECTION_ID = os.getenv("BQ_CONNECTION_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
AIRFLOW_DOCKER_URL = os.getenv("AIRFLOW_DOCKER_URL")
HOST_SRC_DIR = os.getenv("HOST_SRC_DIR")

TAXI_COLORS = [c.strip() for c in os.getenv("TAXI_COLOR", "yellow,green").split(",") if c.strip()]


START_DATE = datetime(2019, 1, 1)
# END_DATE = datetime(2021,7, 1)

GAP_DATES = {
    "start": datetime(2021, 8, 1).replace(tzinfo=None),
    "end": datetime(2026, 8, 1).replace(tzinfo=None)
}

@task(task_id="skip_gap_dates",pool="ingestion_pool")
def check_gap_dates(logical_date: datetime):        # Airflow automatically injects the active DAG run's logical_date because the arg name is logical_date. This is a special Airflow feature that allows tasks to access the execution context without explicitly passing it.
    """
    Checks if the current Airflow run date falls within the ignored gap window.
    If true, raises AirflowSkipException to skip downstream tasks instantly.
    """
    run_date = logical_date.replace(tzinfo=None)
    
    if GAP_DATES["start"] <= run_date <= GAP_DATES["end"]:

        raise AirflowSkipException(
            f"Run date {run_date.strftime('%Y-%m-%d')} falls inside the gap window "
            f"({GAP_DATES['start'].strftime('%Y-%m-%d')} to {GAP_DATES['end'].strftime('%Y-%m-%d')}). Skipping."
        )
@task(task_id="skip_backfill_dates",trigger_rule=TriggerRule.NONE_FAILED)
def current_run_is_last(logical_date: datetime, **context):

    run_date = context["data_interval_end"].replace(tzinfo=None)
   

    today = datetime.today().replace(day=1).strftime('%Y-%m-%d')
    print(run_date," - ", today)
    print(context["data_interval_end"])

    if run_date.strftime('%Y-%m-%d') != today:
        raise AirflowSkipException(
                    f"Run date {run_date.strftime('%Y-%m-%d')} is not today "
                    f"Skipping."
                )

    


DEFAULT_ARGS = {
    "owner": "ishak_tobbi",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    'execution_timeout': timedelta(minutes=20),  # Fails task if it takes longer than 1 hour
    'on_failure_callback': on_failure_slack_alert, # 🟢 Auto-alert on final failure
    'sla_miss_callback': on_sla_miss_slack_alert, # 🟡 Auto-alert on SLA miss

}

# ----------------------------------------------------------------------
# 1. DAG Definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="pipeline_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingest raw taxi datasets to GCS, expose via BigLake, and refresh cache.",
    schedule="@monthly",
    start_date=START_DATE,
    # end_date=END_DATE,
    max_active_runs=6,
    catchup=True,
) as dag:

    base_env = {
        "ENVIRONMENT": "development",
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
        "GOOGLE_APPLICATION_CREDENTIALS": "/tmp/gcp_creds/application_default_credentials.json",
        "EXECUTION_DATE": "{{ ds }}",
    }

    # Guardrail Task
    skip_gap_dates = check_gap_dates()

    # Task 1: Dynamically extract raw streams via Docker containers
    run_extractor_container = DockerOperator.partial(
        task_id="execute_gcs_extractor",
        pool="ingestion_pool",
        image="ingestion:latest",
        auto_remove=True,
        docker_url=AIRFLOW_DOCKER_URL,
        network_mode="orchestrator_default",
        tty=True,
        mount_tmp_dir=False,
        mounts=[
            {
                "source": "orchestrator_gcp-creds",
                "target": "/tmp/gcp_creds",
                "type": "volume",
                "read_only": True,
            },
            {
                "source": HOST_SRC_DIR, 
                "target": "/app/src",
                "type": "bind",
                "read_only": True,
            },
        ],
    ).expand(
        
        # sla=[timedelta(minutes=15)],  # 🟡 SLA: 15 minutes per color extraction
        environment=[{**base_env, "TAXI_COLOR": color} for color in TAXI_COLORS]
    )

    # Task 2: Maintain BigLake External Partitioned Tables
    load_raw_taxi_to_bq = BigQueryCreateExternalTableOperator.partial(
        task_id="create_biglake_external_table",
    ).expand_kwargs(
        [
            {
                "table_resource": {
                    "tableReference": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": f"{color}_taxi_trips",
                    },
                    "maxStaleness": "0:30:0",
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/*"],
                        "metadataCacheMode": "MANUAL",
                        "connectionId": f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/connections/{BQ_CONNECTION_ID}",
                        "hivePartitioningOptions": {
                            "mode": "AUTO",
                            "sourceUriPrefix": f"gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/",
                            "requirePartitionFilter": False,
                        },
                    },
                }
            }
            for color in TAXI_COLORS
        ]
    )

    # Task 3: Refresh BigLake metadata cache AND emit the Dataset outlet
    refresh_bq_cache = BigQueryInsertJobOperator.partial(
        task_id="refresh_biglake_metadata",
        
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        # outlets=[TAXI_RAW_DATASET],  # 🔑 <--- Triggers DAG 2 upon completion!
    ).expand_kwargs(
        [
            {
                # "sla": timedelta(minutes=5),  # 🟡 SLA: 5 minutes per color cache refresh
                "configuration": {
                    "query": {
                        "query": f"""
                            CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(
                            '{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{color}_taxi_trips', 
                            ['gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/year={{{{ logical_date.strftime('%Y') }}}}/month={{{{ logical_date.strftime('%m') }}}}/*']
                            );
                        """,
                        "useLegacySql": False,
                    }
                }
            }
            for color in TAXI_COLORS
        ]
    )

    # Task : partition MERGE into BigQuery control table
    update_control_table = BigQueryInsertJobOperator(
        task_id="update_control_table",
        sla=timedelta(minutes=5),  # 🟡 SLA: 5 minutes for control table update
        configuration={
            "query": {
                "query": f"""
                    MERGE INTO `{GCP_PROJECT_ID}.control.ingestion_partition_control` T
                    USING (
                        SELECT 
                            CONCAT(CAST({{{{ logical_date.strftime('%Y') }}}} AS STRING), '-', LPAD(CAST({{{{ logical_date.strftime('%m') }}}} AS STRING), 2, '0')) AS partition_key,  -- Templated to current DAG run date (e.g., '2024-03')
                            'LOADED' AS status,
                            CURRENT_TIMESTAMP() AS updated_at
                    ) S
                    ON T.partition_key = S.partition_key
                    WHEN MATCHED THEN
                        UPDATE SET 
                            status = S.status,
                            updated_at = S.updated_at
                    WHEN NOT MATCHED THEN
                        INSERT (partition_key, status, updated_at)
                        VALUES (S.partition_key, S.status, S.updated_at);
                """,
                "useLegacySql": False,
            }
        },
    )

    # gate_last_run = ShortCircuitOperator(
    #     task_id="gate_last_run",
    #     python_callable=current_run_is_last,
    #     trigger_rule=TriggerRule.NONE_FAILED, # Runs on Success OR Skipped
    # )

    gate_last_run = current_run_is_last()

    trigger_transformation_dag = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="pipeline_transformation",
        wait_for_completion=False,          # Fire and forget once DAG 1 finishes
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, # Runs on Success OR Skipped
        logical_date="{{ logical_date }}",
    )

    # Ingestion Pipeline Dependencies
    skip_gap_dates >> run_extractor_container >> load_raw_taxi_to_bq >> refresh_bq_cache >> update_control_table >> gate_last_run >> trigger_transformation_dag