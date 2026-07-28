from datetime import datetime, timedelta
import os
from pathlib import Path
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
# Import Cosmos core components
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig,ExecutionConfig,ExecutionMode,LoadMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping,GoogleCloudOauthProfileMapping


# 0. Read your local Mac/Docker environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
BIGQUERY_TABLE_NAME = os.getenv('BIGQUERY_TABLE_NAME')
GCS_FILE_PATH = os.getenv('GCS_FILE_PATH')
AIRFLOW_DOCKER_URL = os.getenv('AIRFLOW_DOCKER_URL', 'unix://var/run/docker.sock')

TAXI_COLOR = os.getenv('TAXI_COLOR')

DBT_PROJECT_PATH = Path("/opt/airflow/dbt_project")

DBT_IMAGE_URL = os.getenv('DBT_IMAGE_URL')  # <-- This is the image we built and pushed to Artifact Registry

# 1. Pipeline Default Settings
default_args = {
    'owner': 'data_engineering_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
}

# 2. Define the Pipeline Structural Workflow w
with DAG(
    dag_id='old_pipeline',
    default_args=default_args,
    description='Triggers our isolated custom Python container to extract data to GCS',
    schedule_interval='@monthly',  # Set to None for manual testing; can change to '@daily' later
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 3, 1),
    catchup=True,  # This allows Airflow to backfill for all past months since the start_date
) as dag:
    
    taxi_colors = TAXI_COLOR.split(",")
    # 1. Define the base environment settings shared by EVERY parallel run
    base_env = {
        'ENVIRONMENT': 'development',
        'GCP_PROJECT_ID': GCP_PROJECT_ID,
        'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        # 'GCS_FILE_PATH': GCS_FILE_PATH,
        'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/gcp_creds/application_default_credentials.json',
        'EXECUTION_DATE': '{{ ds }}'
    }

    # 3. The Master Task: Run our isolated container
    run_extractor_container = DockerOperator.partial(
        task_id='execute_gcs_extractor',
        
        # Tell Airflow which custom image we built earlier to look for
        image='ingestion:latest',
        
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
                 # we sping a docker without script, and create a shared volume between the script and the docker operator, thus we can modify the script and docker operator still run it without rebuild each time (this is for dev only).
                'source': '/Users/ishak/Desktop/Data Engineer/Project/src',     # must be the absolute path to your src directory on your local machine, it is not the path inside the container, it is the path on your host machine, and we will map it to the container's working directory.
                'type': 'bind',         # we use bind when we want to share data from the host machine, it is not managed by docker, it is just a direct link to the host file system, and it is not persistent, if we delete the file on the host, it will be deleted in the container as well.
                'target': '/app/src',       # Maps directly over the container's working directory
                'read_only': False
            }
        ],
        # 2. Tell Google's underlying libraries exactly where to look for that file
        # In case your script needs extra environment configuration variables

        # environment={
        #     'ENVIRONMENT': 'development',
        #     'GCP_PROJECT_ID': GCP_PROJECT_ID,
        #     'GCS_BUCKET_NAME': GCS_BUCKET_NAME,
        #     'GCS_FILE_PATH': GCS_FILE_PATH,
        #     'TAXI_COLOR': TAXI_COLOR,
        #     'EXECUTION_DATE': '{{ ds }}',
        #     'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/gcp_creds/application_default_credentials.json'
        # }
    ).expand(
        # 🟢 This maps a unique TAXI_COLOR env variable to each split container runtime!
        environment=[{**base_env, "TAXI_COLOR": color} for color in taxi_colors]
    )


    load_raw_taxi_to_bq = GCSToBigQueryOperator.partial(
        task_id='load_taxi_to_bigquery',
        bucket=GCS_BUCKET_NAME,
        source_format='PARQUET',
        schema_update_options=['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'],       # allow new columns to be added to the table, and allow existing columns to be relaxed (e.g., from REQUIRED to NULLABLE)
        write_disposition='WRITE_APPEND', # or 'WRITE_TRUNCATE' if you want to overwrite the table each time, or 'WRITE_EMPTY' if you want to ensure the table is empty before writing
        autodetect=True,                  # BigQuery reads the string schemas automatically
        create_disposition='CREATE_IF_NEEDED' # or 'CREATE_NEVER' if you want to ensure the table already exists
    ).expand_kwargs( # 🟢 Notice we changed .expand to .expand_kwargs
        # We use a clean Python list comprehension to zip keys and values together 
        [
            {
                "source_objects": [f"raw/taxi/{color}/year={{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}}}/month={{{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}}}/data.parquet"],
                "destination_project_dataset_table": f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{color}_taxi_trips"
            }
            for color in taxi_colors
        ]
    )
   
    

    
        

    # using cosmos, it runs dbt seeds automatically.

    # transform_data = DbtTaskGroup(      # as a visual box , we can see a task for each model in dbt_project yamland all form a group.
       
    #     group_id="dbt_transformations", # a name of the group, it is not the name of the task.
        
    #     execution_config=ExecutionConfig(
    #         # we installed dbt in a venv in airflow, and use local execution mode, so we need to tell cosmos where to find the dbt executable in the venv.
    #         execution_mode=ExecutionMode.LOCAL, # VENV has conflict with executable dbt, cosmos looking for it in temp, while it exists outside.
    #         dbt_executable_path="/home/airflow/.local/bin/dbt", # it wont work because current airflow install dbt in venv.
    #     ),
        
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_MANIFEST, # <-- This tells Cosmos to use the dbt manifest.json file to determine which models to run
    #         emit_datasets=False,  # this is a flag to tell cosmos to not emit datasets in the dbt_project, it is useful when we don't want to create datasets in the dbt_project, we just want to run the models and not create datasets.
    #     ),

    #     operator_args={
    #         "install_deps": True, # <-- This tells Airflow to run 'dbt deps' inside its container automatically!
    #     },

    #     project_config=ProjectConfig(
    #         DBT_PROJECT_PATH,
    #         manifest_path="opt/airflow/dbt/target/manifest.json" # this is the path to the manifest.json file that is generated by dbt when we run dbt compile or dbt run, it is used by cosmos to know which models to run and which models to skip.

    #     ),     # the path to dbt folders volume mapped to airflow in docker-compose.
       
    #     profile_config=ProfileConfig(
    #         profile_name="dbt_project", # Must match the profile name in dbt_project.yml
    #         target_name="dev",          # it determines which datasets to use in the profile, it is defined in profiles.yml, we can have multiple targets for different environments, like dev, prod, test, etc.
         
    #         # i used before GoogleCloudServiceAccountDictProfileMapping, it expects me to pass the json content in the connection and not the file path.

    #         profile_mapping=GoogleCloudOauthProfileMapping(    # or use profiles.yaml with its path in the machine, cosmos than use it to pass the data connection needed.
    #             conn_id="google_cloud_default",
    #             profile_args={
    #                 "project": f"{GCP_PROJECT_ID}",
    #                 "dataset": f"{BIGQUERY_DATASET}"
    #             },   
    #         ),
            
    #     )
    # )

    # 4. Set our task into motion
    run_extractor_container >> load_raw_taxi_to_bq  #>> transform_data





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
    #############

    # Move data to bigquery 

    # move_date_to_bq = GCSToBigQueryOperator(
    #       task_id='move_data_to_bq',
    #       bucket=GCS_BUCKET_NAME,
    #       source_objects=[GCS_FILE_PATH],
    #       destination_project_dataset_table=f'{GCP_PROJECT_ID}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}'

    # )

    ###########

    # Create External table from data in gcloud storage.

    # create_gcs_external_table = BigQueryCreateExternalTableOperator(
    #     task_id="create_external_table",
        
    #     # bucket=GCS_BUCKET_NAME,
    #     # source_objects=[GCS_FILE_PATH],   # Relative path inside bucket (without gs:// prefix)
    #     # destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE_NAME}',
        
    #     gcp_conn_id="google_cloud_default",
       
    #     # source_format="PARQUET",
    #     # autodetect=True
    
    #     table_resource={
    #         "tableReference": {
    #             "projectId": GCP_PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": BIGQUERY_TABLE_NAME,
    #         },
    #         "externalDataConfiguration": {
    #             "autodetect": True,  # 👈 This forces BigQuery to scan the files and build the schema
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{GCS_BUCKET_NAME}/{GCS_FILE_PATH}"] 
    #         }
    #     }
    # )

   

