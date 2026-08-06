import os
import sys
from pathlib import Path
import pendulum
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from airflow.models import DagBag, DagRun, MappedOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import DagRunState

from cosmos import DbtTaskGroup, ExecutionMode, LoadMode

# ------------------------------------------------------------------------------
# 0. Mock Environment Setup
# ------------------------------------------------------------------------------
os.environ["GCP_PROJECT_ID"] = "test-gcp-project"
os.environ["GCP_REGION"] = "europe-west3"
os.environ["BQ_CONNECTION_ID"] = "test-bq-conn"
os.environ["GCS_BUCKET_NAME"] = "test-gcs-bucket"
os.environ["BIGQUERY_DATASET"] = "test_dataset"
os.environ["AIRFLOW_DOCKER_URL"] = "unix://var/run/docker.sock"
os.environ["HOST_SRC_DIR"] = "/tmp/test_src"
os.environ["TAXI_COLOR"] = "yellow,green"

# - used to ensure that the orchestrator/dags directory is in the Python path for imports so we can import functions from dag file as modules.
DAG_DIR = Path(__file__).parent.parent / "dags"   
# sys.path.insert(0, str(DAG_DIR))  # Ensure DAGs are importable for testing

# scope="session" means this fixture is created once per test session, each function calling it using a saved reference to the same DagBag instance, rather than creating a new one for each test function.
@pytest.fixture(scope="session")
def dag_bag():
    """Loads all DAGs once for the test session."""
    dag_folder = os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)

# ==============================================================================
# 1. PARSING & IMPORT TESTS
# ==============================================================================

def test_no_import_errors(dag_bag):
    """Verify that all DAG files parse with zero Python errors or syntax failures."""
    assert len(dag_bag.import_errors) == 0, f"Import Errors found: {dag_bag.import_errors}"


def test_all_expected_dags_loaded(dag_bag):
    """Verify all 4 core pipeline DAGs exist in the DagBag."""
    expected_dags = [
        "pipeline_ingestion",
        "pipeline_transformation",
        "saved_queries_loader",
        "static_models_loader",
    ]
    for dag_id in expected_dags:
        # dag_bag.get_dag(dag_id) will return None if the DAG is not found
        assert dag_bag.get_dag(dag_id) is not None, f"DAG '{dag_id}' was not found in DagBag!"      # if assertion False then python prints what comes after the comma, ...


# ==============================================================================
# 2. DAG CONFIGURATION & GOVERNANCE TESTS
# ==============================================================================

def test_ingestion_dag_configuration(dag_bag):
    """Verify Ingestion DAG schedule, catchup, owner, and max active runs."""
    dag = dag_bag.get_dag("pipeline_ingestion")

    assert dag.schedule_interval == "@monthly"
    assert dag.catchup is True
    assert dag.max_active_runs == 3
    assert dag.default_args.get("owner") == "ishak_tobbi"
    assert dag.default_args.get("retries") == 2
    assert dag.start_date == pendulum.datetime(2019, 1, 1)  # airflow uses pendulum for datetime objects, so we need to compare with a pendulum datetime object (pendilum include tz even if not specified in datetime().
    assert dag.end_date == None

def test_transformation_dag_configuration(dag_bag):
    """Verify Transformation DAG schedule, catchup, owner, and max active runs."""
    dag = dag_bag.get_dag("pipeline_transformation")

    assert dag.schedule_interval == None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args.get("owner") == "ishak_tobbi"
    assert dag.default_args.get("retries") == 2
    assert dag.start_date == None
    assert dag.end_date == None


def test_downstream_dags_are_manual(dag_bag):
    """Ensure transformation, saved query, and static DAGs rely solely on triggers or manual runs."""
    manual_dags = [
        "pipeline_transformation",
        "saved_queries_loader",
        "static_models_loader",
    ]
    for dag_id in manual_dags:
        dag = dag_bag.get_dag(dag_id)
        assert dag.schedule_interval is None, f"{dag_id} must have schedule=None!"
        assert dag.catchup is False, f"{dag_id} must have catchup=False!"


# ==============================================================================
# 3. GUARDRAIL TASK UNIT TESTS
# ==============================================================================

def test_gap_date_task_function_logic(dag_bag):
    """Verify skip_gap_dates raises AirflowSkipException during gap dates and passes valid dates."""
    dag = dag_bag.get_dag("pipeline_ingestion")
    task = dag.get_task("skip_gap_dates")

    # 1. Date inside gap window (2021-08-01 to 2026-08-01) must raise AirflowSkipException
    gap_date = datetime(2022, 5, 1)
    with pytest.raises(AirflowSkipException, match="falls inside the gap window"):
        task.python_callable(logical_date=gap_date)  # Should raise AirflowSkipException

    # 2. Date outside gap window must execute normally
    valid_date = datetime(2020, 6, 1)

    result = task.python_callable(logical_date=valid_date)
    assert result is None, f"Expected skip gap task to run normally by returning none (dont skip date) for valid date {valid_date}"  # Should complete without error


# ==============================================================================
# 4. TASK MAPPING & DEPENDENCY CHAIN TESTS
# ==============================================================================

def test_ingestion_dag_task_dependencies_and_mapping(dag_bag):
    """Verify mapped tasks and the complete dependency order of the ingestion pipeline."""
    dag = dag_bag.get_dag("pipeline_ingestion")

    skip_gap_dates = dag.get_task("skip_gap_dates")
    extractor = dag.get_task("execute_gcs_extractor")
    external_table = dag.get_task("create_biglake_external_table")
    refresh_cache = dag.get_task("refresh_biglake_metadata")
    update_control = dag.get_task("update_control_table")
    trigger_transformation = dag.get_task("trigger_transformation_dag")

    # Check task mapping configuration
    assert isinstance(extractor, MappedOperator)
    assert isinstance(external_table, MappedOperator)
    assert isinstance(refresh_cache, MappedOperator)

    # Check dependency sequence

    # assert "execute_gcs_extractor" in [t.task_id for t in external_tb.upstream_list]

    # skip_gap_dates >> run_extractor_container
    assert extractor.upstream_task_ids == {"skip_gap_dates"}, f"Expected extractor upstream to be skip_gap_dates, but got {extractor.upstream_task_ids}"

    # run_extractor_container >> load_raw_taxi_to_bq
    assert external_table.upstream_task_ids == {"execute_gcs_extractor"}, f"Expected external_table upstream to be execute_gcs_extractor, but got {external_table.upstream_task_ids}"

    # load_raw_taxi_to_bq >> refresh_bq_cache
    assert refresh_cache.upstream_task_ids == {"create_biglake_external_table"}, f"Expected refresh_cache upstream to be create_biglake_external_table, but got {refresh_cache.upstream_task_ids}"

    # refresh_bq_cache >> update_control_table
    assert update_control.upstream_task_ids == {"refresh_biglake_metadata"}, f"Expected update_control upstream to be refresh_biglake_metadata, but got {update_control.upstream_task_ids}"

    # update_control_table >> trigger_transformation_dag
    assert trigger_transformation.upstream_task_ids == {"update_control_table"}, f"Expected trigger_transformation upstream to be update_control_table, but got {trigger_transformation.upstream_task_ids}"


def test_ingestion_trigger_transformation_chain(dag_bag):
    """Verify pipeline_ingestion correctly triggers pipeline_transformation."""
    dag = dag_bag.get_dag("pipeline_ingestion")
    trigger_task = dag.get_task("trigger_transformation_dag")

    assert trigger_task is not None
    assert trigger_task.trigger_dag_id == "pipeline_transformation"

def test_transformation_trigger_saved_query_chain(dag_bag):
    """Verify pipeline_transformation correctly triggers saved_queries_loader."""

    dag = dag_bag.get_dag("pipeline_transformation")

    trigger_saved_queries_task = dag.get_task("trigger_analytics_dags.trigger_saved_queries_loader_dag")
    trigger_update_dashboard_task = dag.get_task("trigger_analytics_dags.trigger_update_dashboard_dag")

    assert trigger_saved_queries_task is not None
    assert trigger_saved_queries_task.trigger_dag_id == "saved_queries_loader"

    assert trigger_update_dashboard_task is not None
    assert trigger_update_dashboard_task.trigger_dag_id == "update_dashboard"


# ==============================================================================
# 5. SAFEGUARD SENSOR / SHORT-CIRCUIT TESTS
# ==============================================================================

def test_safeguard_tasks_positioning_as_dag_roots(dag_bag):
    """Verify ShortCircuitOperators are the absolute entry points for downstream DAGs."""
    transformation_dag = dag_bag.get_dag("pipeline_transformation")
    saved_queries_dag = dag_bag.get_dag("saved_queries_loader")
    dashboard_dag = dag_bag.get_dag("update_dashboard")

    assert "wait_until_ingestion_finished" in [t.task_id for t in transformation_dag.roots]
    assert "wait_until_transformation_finished" in [t.task_id for t in saved_queries_dag.roots]
    assert "wait_until_transformation_finished" in [t.task_id for t in dashboard_dag.roots]


def test_ingestion_is_finished_callable(dag_bag):
    """Unit test ingestion_is_finished short-circuit callable logic."""
    dag = dag_bag.get_dag("pipeline_transformation")
    wait_for_ingestions = dag.get_task("wait_until_ingestion_finished")

    # Case A: Ingestion DAG is active (RUNNING or QUEUED) -> Return False (skip downstream)
    mock_active_session = MagicMock()
    mock_active_session.query().filter().count.return_value = 1
    assert wait_for_ingestions.python_callable(session=mock_active_session) is False, f"Expected wait_for_ingestions to return False when no ingestion DAG is running, but got True"

    # Case B: Ingestion DAG is idle -> Return True (proceed)
    mock_idle_session = MagicMock()
    mock_idle_session.query().filter().count.return_value = 0
    assert wait_for_ingestions.python_callable(session=mock_idle_session) is True, f"Expected wait_for_ingestions to return True when no ingestion DAG is running, but got False"


def test_transformation_is_finished_callable(dag_bag):
    """Unit test transformation_is_finished short-circuit callable logic."""
    dag = dag_bag.get_dag("saved_queries_loader")
    wait_for_transformation = dag.get_task("wait_until_transformation_finished")

    # Case A: Transformation DAG is active (RUNNING or QUEUED) -> Return False (skip downstream)
    mock_active_session = MagicMock()
    mock_active_session.query().filter().count.return_value = 1
    assert wait_for_transformation.python_callable(session=mock_active_session) is False, f"Expected wait_for_transformation to return False when transformation DAG is running, but got True"

    # Case B: Transformation DAG is idle -> Return True (proceed)
    mock_idle_session = MagicMock()
    mock_idle_session.query().filter().count.return_value = 0
    assert wait_for_transformation.python_callable(session=mock_idle_session) is True, f"Expected wait_for_transformation to return True when transformation DAG is idle, but got False"

    


# ==============================================================================
# 6. TEMPLATE & JINJA SQL RENDERING TESTS
# ==============================================================================

def test_control_table_merge_jinja_rendering(dag_bag):
    """Verify Jinja template rendering for the update_control_table MERGE query."""
    dag = dag_bag.get_dag("pipeline_ingestion")
    task = dag.get_task("update_control_table")

    # Render template using a mock context for 2024-03
    mock_date = datetime(2024, 3, 15, tzinfo=timezone.utc)

    raw_query = task.configuration["query"]["query"]

    rendered_query = task.render_template(
        raw_query,
        {"logical_date": mock_date}
    )

    # Assert Jinja tags were evaluated into correct SQL strings
    assert "CONCAT(CAST(2024 AS STRING), '-', LPAD(CAST(03 AS STRING), 2, '0')) AS partition_key" in rendered_query
    assert "'LOADED' AS status" in rendered_query
    assert "CURRENT_TIMESTAMP() AS updated_at" in rendered_query
    assert "test-gcp-project.control.ingestion_partition_control" in rendered_query


def test_refresh_biglake_cache_jinja_rendering(dag_bag):
    """Verify Jinja rendering in the CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE operator."""
    dag = dag_bag.get_dag("pipeline_ingestion")
    task = dag.get_task("refresh_biglake_metadata")

    mock_date = datetime(2020, 5, 1, tzinfo=timezone.utc)
    raw_query = task.expand_input.value[0]["configuration"]["query"]["query"]

    # Render Jinja string
    rendered_query = task.render_template(
        raw_query,
        {"logical_date": mock_date}
    )

    assert "CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE" in rendered_query
    assert "year=2020/month=05" in rendered_query


# ==============================================================================
# 7. COSMOS DBT TASK GROUP TESTS
# ==============================================================================

def test_cosmos_dbt_task_groups_render():
    """Verify Cosmos loads dbt manifests and renders DbtTaskGroups across all 3 dbt DAGs."""

    transformation_dag_path = DAG_DIR / "pipeline_transformation.py"
    saved_queries_dag_path = DAG_DIR / "saved_queries_loader.py"
    static_dag_path = DAG_DIR / "static_models_loader.py"
    dashboard_dag_path = DAG_DIR / "update_dashboard.py"

    transformation_dag_source = transformation_dag_path.read_text()
    saved_queries_dag_source = saved_queries_dag_path.read_text()
    static_dag_source = static_dag_path.read_text()
    dashboard_dag_source = dashboard_dag_path.read_text()

    assert "load_method=LoadMode.DBT_MANIFEST" in transformation_dag_source, "Expected LoadMode.DBT_MANIFEST in transformation DAG source code"
    assert "load_method=LoadMode.DBT_MANIFEST" in saved_queries_dag_source, "Expected LoadMode.DBT_MANIFEST in saved queries DAG source code"
    assert "load_method=LoadMode.DBT_MANIFEST" in static_dag_source, "Expected LoadMode.DBT_MANIFEST in static models DAG source code"
    assert "load_method=LoadMode.DBT_MANIFEST" in dashboard_dag_source, "Expected LoadMode.DBT_MANIFEST in dashboard DAG source code"

    assert "execution_mode=ExecutionMode.WATCHER" in transformation_dag_source, "Expected ExecutionMode.WATCHER in transformation DAG source code"
    assert "execution_mode=ExecutionMode.WATCHER" in saved_queries_dag_source, "Expected ExecutionMode.WATCHER in saved queries DAG source code"
    assert "execution_mode=ExecutionMode.WATCHER" in static_dag_source, "Expected ExecutionMode.WATCHER in static models DAG source code"
    assert "execution_mode=ExecutionMode.WATCHER" in dashboard_dag_source, "Expected ExecutionMode.WATCHER in dashboard DAG source code"

    assert "tag:saved_query" in saved_queries_dag_source, "Expected 'tag:saved_query' in saved_queries_loader.py"
    assert "tag:static" in static_dag_source, "Expected 'tag:static' in static_models_loader.py"
    assert "tag:dashboard" in dashboard_dag_source, "Expected 'tag:dashboard' in update_dashboard.py"
    assert "tag:daily" in transformation_dag_source, "Expected 'tag:daily' in pipeline_transformation.py"
    