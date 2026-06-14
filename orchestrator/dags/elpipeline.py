from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


# 0. Read your local Mac/Docker environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
BIGQUERY_TABLE_NAME = os.getenv('BIGQUERY_TABLE_NAME')
GCS_FILE_PATH = os.getenv('GCS_FILE_PATH')
AIRFLOW_DOCKER_URL = os.getenv('AIRFLOW_DOCKER_URL', 'unix://var/run/docker.sock')

# 1. Pipeline Default Settings
default_args = {
    'owner': 'data_engineering_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Define the Pipeline Structural Workflow
with DAG(
    dag_id='gcs_data_extraction_pipeline',
    default_args=default_args,
    description='Triggers our isolated custom Python container to extract data to GCS',
    schedule_interval=None,  # Set to None for manual testing; can change to '@daily' later
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    

    # 3. The Master Task: Run our isolated container
    run_extractor_container = DockerOperator(
        task_id='execute_gcs_extractor',
        
        # Tell Airflow which custom image we built earlier to look for
        image='elpipeline:latest',
        
        # Equivalent to typing '--rm' in terminal to clean up the container when done
        auto_remove=True,
        
        # Use the host telephone line (/var/run/docker.sock) we mapped in docker-compose
        docker_url=AIRFLOW_DOCKER_URL,
        # if we use a different machine, the path ll be different, we can set it in .env file.
        
        # Network mode must match your docker-compose network so containers can see each other
        network_mode='orchestrator_default',
        # we add the script container to the same network as the orchestrator, so they can communicate with each other
        
        # This allows Airflow logs to display your Python script's print statements live
        tty=True,
        # it is as -t, what the script prints, airflow can grab it and show in the logs, if we dont have it, we can only see the logs after the script is done.
        
        # 1. Share the key file from Airflow into the new extractor container
        mounts=[
            {
                'source': '/opt/airflow/secrets/google_key.json', # Location inside Airflow
                'target': '/app/secrets/google_key.json',         # Location inside Extractor
                'type': 'bind',
                'read_only': True
            }
        ],
        # 2. Tell Google's underlying libraries exactly where to look for that file
        # In case your script needs extra environment configuration variables
        environment={
            'ENVIRONMENT': 'production',
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
            'GCS_FILE_PATH': GCS_FILE_PATH,
            'GOOGLE_APPLICATION_CREDENTIALS': '/app/secrets/google_key.json'
        }
    )

    create_external_table = BigQueryExecuteQueryOperator(
        task_id='create_bq_external_table',
        sql=f"""
            CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}`
            OPTIONS (
                format = 'PARQUET',
                uris = ['gs://{GCS_BUCKET_NAME}/{GCS_FILE_PATH}']
            );
        """,
        use_legacy_sql=False,

        gcp_conn_id='google_cloud_default', # Airflow uses its built-in GCP connection system
        # we create the connection in the airflow ui, when it need to do something in the cloud, it will look for the connection with the name we specify here, and use the credentials we set up in that connection to authenticate and do the work in the cloud.
    
    )

    # 4. Set our task into motion
    run_extractor_container >> create_external_table