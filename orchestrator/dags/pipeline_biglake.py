from datetime import datetime, timedelta
from docker.types import Mount  # 🟢 MUST use the official Docker types!
import os
from pathlib import Path
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from cosmos import DbtTaskGroup,ProjectConfig,ProfileConfig,RenderConfig,LoadMode,ExecutionConfig,ExecutionMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping,GoogleCloudOauthProfileMapping



# 0. Read your local Mac/Docker environment variables
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_REGION = os.getenv('GCP_REGION')
BQ_CONNECTION_ID = os.getenv('BQ_CONNECTION_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
BIGQUERY_TABLE_NAME = os.getenv('BIGQUERY_TABLE_NAME')
GCS_FILE_PATH = os.getenv('GCS_FILE_PATH')
AIRFLOW_DOCKER_URL = os.getenv('AIRFLOW_DOCKER_URL', 'unix://var/run/docker.sock')

TAXI_COLOR = os.getenv('TAXI_COLOR')


DBT_IMAGE_URL = os.getenv('DBT_IMAGE_URL')

# 1. HOST PATHS (Where your local code sits on your host machine so Airflow can parse it)
AIRFLOW_HOST_MANIFEST = "/opt/airflow/dbt_project/target/manifest.json"
AIRFLOW_HOST_DBT_PROJECT = "/opt/airflow/dbt_project"
AIRFLOW_HOST_DBT_EXE = "/home/airflow/dbt_venv/bin/dbt"

# 1. Pipeline Default Settings
default_args = {
    'owner': 'ishak_tobbi',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2019, 3, 1)

# 1. Define the guard function that checks the Airflow execution context
def skip_historical_months(context):
    # Airflow provides the exact run date inside the 'logical_date' context
    logical_date = context['logical_date']
    
    # If the current run's year/month doesn't match our target end date, SKIP IT
    if not (logical_date.year == END_DATE.year and logical_date.month == END_DATE.month):
        raise AirflowSkipException(
            f"Skipping dbt execution for historical month: {logical_date.strftime('%Y-%m')}"
        )


# 2. Define the Pipeline Structural Workflow w
with DAG(
    dag_id='pipeline_biglake',
    default_args=default_args,
    description='Triggers our isolated custom Python container to extract data to GCS',
    schedule_interval='@monthly',  # Set to None for manual testing; can change to '@daily' later
    start_date=START_DATE,
    end_date=END_DATE,
    max_active_runs=9,
    catchup=True,  # This allows Airflow to backfill for all past months since the start_date
) as ingestion_dag:
    

    

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

        pool= "ingestion_pool",
        
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
        
    ).expand(
        # 🟢 This maps a unique TAXI_COLOR env variable to each split container runtime!
        environment=[{**base_env, "TAXI_COLOR": color} for color in taxi_colors]
    )


    load_raw_taxi_to_bq = BigQueryCreateExternalTableOperator.partial(
        task_id='create_biglake_external_table',
        # bucket=GCS_BUCKET_NAME,
        # source_format='PARQUET',
        
        
        # 🟢 THE BIGLAKE SPEED BOOST:
        # Caches GCS file metadata internally so queries run significantly faster
        # metadata_cache_mode="AUTOMATIC",
        # Max time (in milliseconds) BigQuery can use cached metadata before refreshing 
        # max_staleness="0-0 0:30:0", # 30 minutes cache boundary
    ).expand_kwargs(
        [
            {
             
            # 🟢 3. Destination tables: green_taxi_trips and yellow_taxi_trips
            # "destination_project_dataset_table": f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{color}_taxi_trips",

            # 🟢 THE AIRFLOW 2.10 COMPLIANT BIGLAKE CACHE KEY:
            # We map the metadata caching properties down into the table_resource schema definition

            

            "table_resource": {
                "tableReference": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{color}_taxi_trips",    # the table name we want to create.
                },
                "maxStaleness": "0:30:0",       # refresh the metadata of the storage.
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/*"],   # source of the data in storage, hive partition addition not needed, BQ handle it automatic.
                    
                    # BigLake Cache Optimization
                    "metadataCacheMode": "MANUAL",
                    
                    # connection needed so BQ can connect to the cloud storage, we create it (as a service acc) in UI console, give it permissions in storage, point it here.
                    "connectionId": f"projects/{GCP_PROJECT_ID}/locations/{GCP_REGION}/connections/{BQ_CONNECTION_ID}",
                                    
                    
                    # Hive Partition Directory Resolution Rules (CamelCase API Specs)
                    "hivePartitioningOptions": { 
                        "mode": "AUTO",     # BQ automatic assign columns names and types of the partition month and year.
                        "sourceUriPrefix": f"gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/",     # the prefix of the data in the storage, BQ then read what comes after and use it as filters.
                        "requirePartitionFilter": False     # when False, dbt can query the data without filter month and year, it can query all of it.
                    }
                }
            }
        }
            for color in taxi_colors
        ]
    )

    # using cosmos, it runs dbt seeds automatically.

    transform_data = DbtTaskGroup(      # as a visual box , we can see a task for each model in dbt_project yamland all form a group.
        group_id="dbt_transformations", # a name of the group, it is not the name of the task.
        execution_config=ExecutionConfig(
            # we installed dbt in a venv in airflow, and use local execution mode, so we need to tell cosmos where to find the dbt executable in the venv.
            execution_mode=ExecutionMode.WATCHER, # VENV has conflict with executable dbt, cosmos looking for it in temp, while it exists outside.
            dbt_executable_path= AIRFLOW_HOST_DBT_EXE, # it wont work because current airflow install dbt in venv.
            setup_operator_args={
                "pool": "dbt_pool",   # <--- This applies the 3-slot limit to the producer which is doing all dbt tasks!
            }
        ),
        
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST, # <-- This tells Cosmos to use the dbt manifest.json file to determine which models to run
            emit_datasets=False,  # this is a flag to tell cosmos to not emit datasets in the dbt_project, it is useful when we don't want to create datasets in the dbt_project, we just want to run the models and not create datasets.
            select=["tag:daily"],    # specify which models to run based on a tag defined in dbt_project yaml    
        ),



        operator_args={
            # "install_deps": True, # <-- This tells Airflow to run 'dbt deps' inside its container automatically!
            # "virtualenv_dir": Path("/home/airflow/dbt_venv/"),
            # "py_requirements": ["dbt-bigquery==1.9.2"],
        
        },

        project_config=ProjectConfig(
            dbt_project_path= AIRFLOW_HOST_DBT_PROJECT,
            project_name= "dbt_project",
            manifest_path= AIRFLOW_HOST_MANIFEST, # this is the path to the manifest.json file that is generated by dbt when we run dbt compile or dbt run, it is used by cosmos to know which models to run and which models to skip.
            install_dbt_deps=False,

            dbt_vars={
                "run_date": "{{ logical_date.strftime('%Y-%m-%d') }}",
                "run_year": "{{ logical_date.strftime('%Y') }}",
                "run_month": "{{ logical_date.strftime('%m') }}"
            }
        ),     # the path to dbt folders volume mapped to airflow in docker-compose.
       
        profile_config=ProfileConfig(
            profile_name="dbt_project", # Must match the profile name in dbt_project.yml
            target_name="dev",          # it determines which datasets to use in the profile, it is defined in profiles.yml, we can have multiple targets for different environments, like dev, prod, test, etc.
         
            # i used before GoogleCloudServiceAccountDictProfileMapping, it expects me to pass the json content in the connection and not the file path.

            profile_mapping=GoogleCloudOauthProfileMapping(    # or use profiles.yaml with its path in the machine, cosmos than use it to pass the data connection needed.
                conn_id="google_cloud_default",
                profile_args={
                    "project": f"{GCP_PROJECT_ID}",
                    "dataset": f"{BIGQUERY_DATASET}",
                    "location": GCP_REGION,
                },   
            ),
            
        )
    )

    # Refresh ONLY the path created/updated by this specific run
    refresh_bq_cache = BigQueryInsertJobOperator.partial(
        task_id="refresh_biglake_metadata",
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        # configuration={
        #     "query": {
        #         # 🟢 We pass the table name AND the specific run's GCS directory with a wildcard!
        #         "query": f"""
        #             CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(
        #             'elpipeline.ishak_tobbi_dataset.your_table_name', 
        #             ['gs:///current data path/year={{ logical_date.strftime('%Y') }}/month={{ logical_date.strftime('%m') }}/*']
        #             );
        #         """,
        #         "useLegacySql": False,
        #     }
        # }
    ).expand_kwargs( # 🟢 Notice we changed .expand to .expand_kwargs
        # We use a clean Python list comprehension to zip keys and values together 
        [
            {
                "configuration":{
                    "query": {
                        # 🟢 We pass the table name AND the specific run's GCS directory with a wildcard!
                        "query": f"""
                            CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(
                            '{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{color}_taxi_trips', 
                            ['gs://{GCS_BUCKET_NAME}/raw/taxi/{color}/year={{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}}}/month={{{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}}}/*']
                            );
                        """,
                        "useLegacySql": False,
                    }
                }
            }
            for color in taxi_colors
        ]
    )


   

    run_extractor_container >> load_raw_taxi_to_bq >> refresh_bq_cache >> transform_data
   
    


