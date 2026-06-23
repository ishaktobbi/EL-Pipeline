from datetime import datetime, timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Import Cosmos core components
# from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
# from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping


# 0. Read your local Mac/Docker environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
BIGQUERY_TABLE_NAME = os.getenv('BIGQUERY_TABLE_NAME')
GCS_FILE_PATH = os.getenv('GCS_FILE_PATH')
AIRFLOW_DOCKER_URL = os.getenv('AIRFLOW_DOCKER_URL', 'unix://var/run/docker.sock')

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

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
        
        # Force Airflow to boot the local image instantly! without waiting for docker to fetch for latest version.
        # image_pull_policy='Never', 
        
        # Use the host telephone line (/var/run/docker.sock) we mapped in docker-compose
        docker_url=AIRFLOW_DOCKER_URL,
        # if we use a different machine, the path ll be different, we can set it in .env file.
        
        # Network mode must match your docker-compose network so containers can see each other
        network_mode='orchestrator_default',
        # we add the script container to the same network as the orchestrator, so they can communicate with each other
        
        # This allows Airflow logs to display your Python script's print statements live
        tty=True,
        # it is as -t, what the script prints, airflow can grab it and show in the logs, if we dont have it, we can only see the logs after the script is done.
        

        mount_tmp_dir=False, # We will handle our own custom mounts, so disable Airflow's default temp directory mount

        # 1. Mount the shared named volume (same volume that Airflow uses) into the script container
        mounts=[
            {
                'source': 'orchestrator_gcp-creds',  # the name of the volume we defined in docker-compose.yaml
                'target': '/tmp/gcp_creds',
                'type': 'volume',       # we use volume when we want to share data from the docker daemon volume pocket, it is managed by docker, we can use it to share data between containers, and it is persistent, it will not be deleted when the container is deleted.
                'read_only': True
            },
            {
                 # Your absolute Mac path to where your script lives
                'source': '/Users/ishak/Desktop/Data Engineer/Project/src',     # must be the absolute path to your src directory on your local machine, it is not the path inside the container, it is the path on your host machine, and we will map it to the container's working directory.
                'type': 'bind',         # we use bind when we want to share data from the host machine, it is not managed by docker, it is just a direct link to the host file system, and it is not persistent, if we delete the file on the host, it will be deleted in the container as well.
                'target': '/app/src',       # Maps directly over the container's working directory
                'read_only': False
            }
        ],
        # 2. Tell Google's underlying libraries exactly where to look for that file
        # In case your script needs extra environment configuration variables
        environment={
            'ENVIRONMENT': 'development',
            'GCP_PROJECT_ID': GCP_PROJECT_ID,
            'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
            'GCS_FILE_PATH': GCS_FILE_PATH,
            'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/gcp_creds/application_default_credentials.json'
        }
    )

    

    create_gcs_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    bucket=GCS_BUCKET_NAME,
    source_objects=[GCS_FILE_PATH], # Relative path inside bucket (without gs:// prefix)
    source_format="PARQUET",
    destination_project_dataset_table=f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}',
    gcp_conn_id="google_cloud_default",
    autodetect=True, # Let BigQuery infer the schema automatically

    # schema_fields=[
    #     {"name": "VendorID", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "tpep_pickup_datetime", "type": "STRING", "mode": "NULLABLE"},
    #     {"name": "tpep_dropoff_datetime", "type": "STRING", "mode": "NULLABLE"},
    #     {"name": "passenger_count", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "RatecodeID", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
    #     {"name": "PULocationID", "type": "INTEGER", "mode": "NULLABLE"},
    #     {"name": "DOLocationID", "type": "INTEGER", "mode": "NULLABLE"},
    #     {"name": "payment_type", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    #     {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
    # ],

)

    # transform_data = DbtTaskGroup(
    #     group_id="dbt_transformations",
    #     project_config=ProjectConfig(DBT_PROJECT_PATH),
    #     profile_config=ProfileConfig(
    #         profile_name="bigquery_profile", # Must match the profile name in dbt_project.yml
    #         target_name="dev",
    #         profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
    #             conn_id="google_cloud_default", # Reuses your existing ADC container credentials!
    #             profile_args={"dataset": f"{BIGQUERY_DATASET}"},
    #         ),
    #     ),
    #     execution_config=ExecutionConfig(
    #         # Tells Airflow where the dbt executable lives inside its virtual environment
    #         dbt_executable_path="/home/airflow/.local/bin/dbt" 
    #     )
    # )

    # 4. Set our task into motion
    run_extractor_container >> create_gcs_external_table





    # OLD USED OPERATORS : ##########################################################

    # create_external_table = BigQueryExecuteQueryOperator(
    #     task_id='create_bq_external_table',
    #     sql=f"""
    #         CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}`
    #         OPTIONS (
    #             format = 'PARQUET',
    #             uris = ['gs://{GCS_BUCKET_NAME}/{GCS_FILE_PATH}']
    #         );
    #     """,
    #     use_legacy_sql=False,

    #     gcp_conn_id='google_cloud_default', # Airflow uses its built-in GCP connection system
    #     # we create the connection in the airflow ui,

    #     # conn_id : google_cloud_default
    #     # provider : google cloud
    #     # keypath : the path of the shared volume, > /opt/airflow/gcloud/creds.json
    
    # )

    # move_date_to_bq = GCSToBigQueryOperator(
    # task_id='move_data_to_bq',
    # bucket=GCS_BUCKET_NAME,
    # source_objects=[GCS_FILE_PATH],
    # destination_project_dataset_table=f'{GCP_PROJECT_ID}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}'

    # )

