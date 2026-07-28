import os
import io
import gzip
import pytest
import pandas as pd
import pyarrow.parquet as pq
from unittest.mock import MagicMock, patch
from requests.exceptions import ConnectTimeout, ReadTimeout
# from airflow.exceptions import AirflowSkipException

from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden


# Adjust import path to match your project structure
from src.ingestion_v2 import NYCDataIngestor, GCSUploader, AirflowSkipException
from src.utils import parse_date, validate_color


# ==============================================================================
# COMPONENT 1: NYCDataIngestor
# PART 1: Format & Input Validation (Pure Offline / No Mocks)
# ==============================================================================

class TestNYCDataIngestorValidation:
    """Tests static validation methods, input guards, and path/URL generation."""

    # --------------------------------------------------------------------------
    # 1. Taxi Color Validation
    # --------------------------------------------------------------------------
    @pytest.mark.parametrize("input_color, expected", [
        ("YELLOW", "yellow"),
        (" green ", "green"),
    ])
    def test_validate_color_success(self, input_color, expected):
        """Accepts valid colors and normalizes them to lowercase."""
        assert validate_color(input_color) == expected

    @pytest.mark.parametrize("invalid_color", [     # mar.parametrize used so we dont go for it one by one, that way it goes as a loop.
        "pink", "blue", "   ", "", "123"
    ])
    def test_validate_color_failure(self, invalid_color):
        """Raises ValueError for invalid, empty, or unsupported colors."""
        with pytest.raises(ValueError):
            validate_color(invalid_color)

    # --------------------------------------------------------------------------
    # 2. Date Parsing & Calendar Validation
    # --------------------------------------------------------------------------
    def test_parse_date_success(self):
        """Correctly extracts (YYYY, MM) from a valid YYYY-MM-DD string."""
        year, month = parse_date("2026-06-15")
        assert year == "2026"       # used mainly for success, it crashes if false.
        assert month == "06"

    @pytest.mark.parametrize("invalid_format", [
        "15-06-2026",  # DD-MM-YYYY
        "2026/06/15",  # Slashes
        "2026-06",     # Missing day
        "15 June 2026",# Text format
        "20260615",    # Raw digits
    ])
    def test_parse_date_format_failure(self, invalid_format):
        """Raises ValueError when date string does not follow YYYY-MM-DD."""
        with pytest.raises(ValueError, match="Invalid date format"):    # expect the code to throw error with exact type and message.
            parse_date(invalid_format)

    @pytest.mark.parametrize("invalid_calendar_date", [
        "2026-13-15",  # Month 13
        "2026-02-30",  # Feb 30
        "2026-00-10",  # Month 0
    ])
    def test_parse_date_calendar_failure(self, invalid_calendar_date):
        """Raises ValueError when date format is correct but calendar date is impossible."""
        with pytest.raises(ValueError, match="Invalid calendar date"):
            parse_date(invalid_calendar_date)

    # --------------------------------------------------------------------------
    # 3. Null & Type Guardrails
    # --------------------------------------------------------------------------
    @pytest.mark.parametrize("bad_date", [None, 12345, [], {}])
    def test_init_invalid_date_types(self, bad_date):
        """Raises TypeError or ValueError if execution_date is not a string."""
        with pytest.raises((TypeError, ValueError)):
            NYCDataIngestor(execution_date=bad_date, taxi_color="yellow")

    @pytest.mark.parametrize("bad_color", [None, 999, ["yellow"]])
    def test_init_invalid_color_types(self, bad_color):
        """Raises TypeError or ValueError if taxi_color is not a string."""
        with pytest.raises((TypeError, ValueError)):
            NYCDataIngestor(execution_date="2026-06-15", taxi_color=bad_color)

    # --------------------------------------------------------------------------
    # 4. Path & URL Construction
    # --------------------------------------------------------------------------
    def test_path_and_url_formatting(self):
        """Verifies local Parquet filename and source download URL formatting."""
        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")
        
        expected_filename = "yellow_tripdata_2026-06.parquet"
        expected_url = (
            "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
            "yellow/yellow_tripdata_2026-06.csv.gz"
        )
        
        assert ingestor.local_parquet_path == expected_filename
        assert ingestor.url == expected_url


# ==============================================================================
# COMPONENT 1: NYCDataIngestor
# PART 2: Network, Resilience & Streaming (HTTP Mocks)
# ==============================================================================
class TestNYCDataIngestorStreaming:
    """Tests network retry strategy, timeouts, Airflow skips, and streaming file handle safety."""

    # Helper: Creates compressed GZIP CSV bytes for stream mocking
    @staticmethod
    def _create_mock_gz_csv(csv_content: str) -> bytes:
        # 1. Create an empty binary bucket inside RAM
        buf = io.BytesIO()

        # 2. Attach Python's GzipFile compression engine to our RAM bucket
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            # csv_content is a string: "VendorID,tpep_pickup...\n1,2026-06-15..."
            # f.write takes BYTES, so .encode("utf-8") converts string -> raw bytes
            f.write(csv_content.encode("utf-8"))
            # When exiting the 'with' block, gzip flushes and finalizes compressed headers into 'buf'

        buf.seek(0)     # moves the pointer inside the file to index 0.
        return buf


    # --------------------------------------------------------------------------
    # 1. Session Retry & Configuration
    # --------------------------------------------------------------------------
    def test_session_retry_configuration(self):
        """Verifies HTTPAdapter mounts with 3 retries, backoff, and retry status codes."""
        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")
        adapter = ingestor.session.get_adapter("https://")
        
        assert adapter.max_retries.total == 3
        assert adapter.max_retries.backoff_factor == 2
        assert set(adapter.max_retries.status_forcelist) == {429, 500, 502, 503, 504}

    # --------------------------------------------------------------------------
    # 2. Connection & Read Timeouts
    # --------------------------------------------------------------------------

            
    @patch("requests.Session.get")      # MagicMock unplug the real requests.Session.get, and use its fake one.
    def test_connection_timeout_raises_error(self, mock_get):   # mock_get can be named anything, as long as it is in the first arg, it get passed by the decorator.
        """Mock ConnectTimeout on HTTP request -> verifies exception handling."""
        mock_get.side_effect = ConnectTimeout("Connection timed out to GitHub")     # side_effect we control the behavior of the MagicMock, throw an error.
        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")

        with pytest.raises(RuntimeError, match="Connection timed out"):   
            ingestor.extract_and_convert()

    @patch("src.ingestion_v2.pd.read_csv")
    @patch("requests.Session.get")
    def test_read_timeout_midstream_cleans_up_local_file(self, mock_get,mock_read_csv, tmp_path):     # when pytest sees 2nd arg tmp_path, it auto create a temp dir to use in current test. it should be exactly that name.
        """Mock ReadTimeout mid-stream -> closes file handles and removes partial files."""
        # Create a mock response whose raw content throws ReadTimeout during iteration
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response       # python with MagicMock auto uses __enter__() under the hood. it works with or without.

        # mock_response.raw.stream.side_effect, we can assign anything to MagicMock, any arg, function call, anything and it ll adabt.
        mock_read_csv.side_effect = ReadTimeout("Socket read timed out")     # it auto recognise the function name and flag it as an exception, the tested code exception will catch it then.
        

        # Point ingestor to temporary test directory
        target_path = tmp_path / "yellow_tripdata_2026-06.parquet"
        target_path.touch()  # Creates a dummy file on disk BEFORE running the code, and the code ll clean it later

        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")
        ingestor.local_parquet_path = str(target_path)

        with pytest.raises(RuntimeError, match="Connection timed out middle way"):
            ingestor.extract_and_convert()

        # Assert local partial file was cleaned up and does not linger on disk
        assert not os.path.exists(target_path)

    # --------------------------------------------------------------------------
    # 3. Airflow Flow Control (404 & Empty Data)
    # --------------------------------------------------------------------------
    @patch("src.ingestion_v2.pd.read_csv")
    @patch("requests.Session.get")
    def test_http_404_raises_airflow_skip(self, mock_get, mock_read_csv):
        """Mock HTTP 404 response -> raises AirflowSkipException."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value.__enter__.return_value = mock_response

        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")

        with pytest.raises(AirflowSkipException, match="No source data published"):
            ingestor.extract_and_convert()

    @patch("requests.Session.get")
    def test_empty_csv_stream_raises_airflow_skip(self, mock_get, tmp_path):
        """Mock CSV stream returning 0 rows / headers only -> raises AirflowSkipException."""
        # Header only, no data rows
        empty_csv = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count\n"
        gz_bytes = self._create_mock_gz_csv(empty_csv)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = gz_bytes
        mock_get.return_value.__enter__.return_value = mock_response   

        target_path = tmp_path / "yellow_tripdata_2026-06.parquet"
        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")
        ingestor.local_parquet_path = str(target_path)

        with pytest.raises(AirflowSkipException, match="Empty dataset: No data rows found"):
            ingestor.extract_and_convert()

        assert not os.path.exists(target_path)

    # --------------------------------------------------------------------------
    # 4. Stream Success & Parquet Validity
    # --------------------------------------------------------------------------
    @patch("requests.Session.get")
    def test_extract_and_convert_success(self, mock_get, tmp_path):
        """Valid CSV stream -> creates non-empty Parquet file readable by PyArrow."""
        valid_csv = (
            "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance\n"
            "1,2026-06-15 00:00:00,2026-06-15 00:15:00,1,2.5\n"
            "2,2026-06-15 01:00:00,2026-06-15 01:20:00,2,4.1\n"
        )
        gz_bytes = self._create_mock_gz_csv(valid_csv)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = gz_bytes
        mock_get.return_value.__enter__.return_value = mock_response 

        target_path = tmp_path / "yellow_tripdata_2026-06.parquet"
        ingestor = NYCDataIngestor(execution_date="2026-06-15", taxi_color="yellow")
        ingestor.local_parquet_path = str(target_path)

        # Act
        ingestor.extract_and_convert()

        # Assert Parquet file exists and is valid
        assert os.path.exists(target_path)
        assert os.path.getsize(target_path) > 0

        # Read back with PyArrow to verify schema and row count
        table = pq.read_table(target_path)
        assert table.num_rows == 2
        assert "VendorID" in table.column_names
        assert "trip_distance" in table.column_names


# ==============================================================================
# COMPONENT 2: GCSUploader
# PART 1: Format & Input Validation (Pure Offline / No Mocks)
# ==============================================================================

class TestGCSUploaderValidation:

    @patch.object(GCSUploader, "_verify_bucket")
    def test_invalid_project_id_type_raises_type_error(self, mock_verify):
        """Passing non-string for project_id raises TypeError."""
        with pytest.raises(TypeError, match="project_id must be a string"):
            GCSUploader(
                project_id=12345,  # Int instead of str
                bucket_name="my-bucket",
                execution_date="2026-06-15",
                taxi_color="yellow",
            )
    @patch.object(GCSUploader, "_verify_bucket")
    def test_invalid_bucket_name_type_raises_type_error(self, mock_verify):
        """Passing non-string for project_id raises TypeError."""
        with pytest.raises(TypeError, match="bucket_name must be a string"):
            GCSUploader(
                project_id="my_project", 
                bucket_name=1234,   # Int instead of str
                execution_date="2026-06-15",
                taxi_color="yellow",
            )

    @patch.object(GCSUploader, "_verify_bucket")
    def test_empty_project_id_raises_value_error(self, mock_verify):
        """Passing empty or whitespace project_id raises ValueError."""
        with pytest.raises(ValueError, match="project_id cannot be empty"):
            GCSUploader(
                project_id="   ",  # Blank whitespace
                bucket_name="my-bucket",
                execution_date="2026-06-15",
                taxi_color="yellow",
            )

    @patch.object(GCSUploader, "_verify_bucket")
    def test_empty_bucket_name_raises_value_error(self, mock_verify):
        """Passing empty or whitespace project_id raises ValueError."""
        with pytest.raises(ValueError, match="bucket_name cannot be empty."):
            GCSUploader(
                project_id="my-bucket",
                bucket_name="   ",  # Blank whitespace
                execution_date="2026-06-15",
                taxi_color="yellow",
            )

# ==============================================================================
# COMPONENT 2: GCSUploader
# PART 2: Path Formatting & Hive Structuring (Mocked GCS Client)
# ==============================================================================

class TestGCSUploaderFormatting:
    """Tests Hive destination blob formatting and constructor initialization offline."""

    # that way it is efficient to fine specific code functions and methods by referencing the class obj.
    # unlike response.get which python finds it because it is a python module.
    @patch.object(GCSUploader, "_verify_bucket") # or @patch("src.gcs_uploader.GCSUploader._verify_bucket")
    def test_destination_blob_name_hive_partitioning(self, mock_verify):
        # mock_verify here to mock the bucket and not runs the actual _verify_bucket which my raise exceptions.
        """Verifies destination path follows raw/taxi/{color}/year={YYYY}/month={MM}/data.parquet."""
        # Arrange
        mock_client = MagicMock()
        
        # Act
        uploader = GCSUploader(
            project_id="my-gcp-project-123",
            bucket_name="nyc-taxi-data-bucket",
            execution_date="2026-06-15",
            taxi_color="yellow",
            client=mock_client      # used to mock the storage_client and to not runs the actual storage.client()
        )

        # Assert
        expected_blob = "raw/taxi/yellow/year=2026/month=06/data.parquet"
        assert uploader.destination_blob_name == expected_blob
        assert uploader.year == "2026"
        assert uploader.month == "06"


# ==============================================================================
# COMPONENT 2: GCSUploader
# PART 3: Cloud Network & Safe Cleanup (GCS Mocks)
# ==============================================================================

class TestGCSUploaderCloudExecution:
    """Tests GCP bucket exceptions, upload execution, and guaranteed local file cleanup."""

    # --------------------------------------------------------------------------
    # 1. Project & Bucket Exception Handling
    # --------------------------------------------------------------------------
    def test_bucket_not_found_raises_value_error(self):
        """Mock GCP returning NotFound (404) -> raises custom ValueError."""
        mock_client = MagicMock()
        mock_client.get_bucket.side_effect = NotFound("Bucket 'bad-bucket' does not exist")

        with pytest.raises(ValueError, match="does not exist"):
            GCSUploader(
                project_id="my-gcp-project-123",
                bucket_name="non-existent-bucket",
                execution_date="2026-06-15",
                taxi_color="yellow",
                client=mock_client
            )

    def test_permission_denied_raises_permission_error(self):
        """Mock GCP returning Forbidden (403 IAM error) -> raises PermissionError."""
        mock_client = MagicMock()
        mock_client.get_bucket.side_effect = Forbidden("Access Denied to Bucket")

        with pytest.raises(PermissionError, match="Access denied to project/bucket."):
            GCSUploader(
                project_id="my-gcp-project-123",
                bucket_name="restricted-bucket",
                execution_date="2026-06-15",
                taxi_color="yellow",
                client=mock_client
            )

    # --------------------------------------------------------------------------
    # 2. Upload Execution & Idempotency
    # --------------------------------------------------------------------------
    @patch.object(GCSUploader, "_verify_bucket")
    def test_upload_success_and_overwrite(self, mock_verify, tmp_path):
        """Valid local file -> calls upload_from_filename and removes local file."""
        # 1. Arrange: Create a temporary test Parquet file
        local_file = tmp_path / "yellow_tripdata_2026-06.parquet"
        local_file.write_bytes(b"mock_parquet_binary_content")

        # Mock bucket & blob setup
        mock_bucket = MagicMock()
        mock_target_blob = MagicMock()
        
        # Configure mock responses
        mock_verify.return_value = mock_bucket  # we need its mock return when called so we use it for further extra methods calls.
        mock_bucket.blob.return_value = mock_target_blob    # our target blob
        mock_target_blob.exists.return_value = True  # Simulates existing blob for overwrite print
        

        uploader = GCSUploader(
            project_id="my-gcp-project-123",
            bucket_name="nyc-taxi-data-bucket",
            execution_date="2026-06-15",
            taxi_color="yellow",
            # client=mock_client    we dont need it because i actual code, it never get used, assign in code is just kept in memory.
        )

        # 2. Act: Call upload_file (matching your method name!)
        uploader.upload_file(local_file_path=str(local_file))

        # 3. Assert
        # assert_called_once_with used to assert if atleast it has been called with that argument once.
        mock_bucket.blob.assert_called_once_with(uploader.destination_blob_name)
        mock_target_blob.upload_from_filename.assert_called_once_with(str(local_file))

        # Verify the cleanup guarantee: the local file should be deleted by the finally block
        assert not local_file.exists()

    # --------------------------------------------------------------------------
    # 3. Guaranteed Local File Cleanup (`finally` block)
    # --------------------------------------------------------------------------
    @patch.object(GCSUploader, "_verify_bucket")
    def test_upload_failure_guarantees_file_cleanup(self, mock_verify, tmp_path):
        """Network/Storage failure during upload -> local file is STILL safely deleted."""
        # Arrange: Create a temporary local file
        local_file = tmp_path / "yellow_tripdata_2026-06.parquet"
        local_file.write_bytes(b"mock_parquet_binary_content")

        mock_bucket = MagicMock()
        mock_target_blob = MagicMock()
        # Simulate a network crash during GCS upload
        mock_verify.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_target_blob
        mock_target_blob.upload_from_filename.side_effect = Exception("GCS Connection Interrupted")


        uploader = GCSUploader(
            project_id="my-gcp-project-123",
            bucket_name="nyc-taxi-data-bucket",
            execution_date="2026-06-15",
            taxi_color="yellow"
        )

        # Act & Assert exception is raised
        with pytest.raises(Exception, match="GCS Connection Interrupted"):
            uploader.upload_file(local_file_path=str(local_file))

        # Assert: `finally:` block executed and deleted the local Parquet file from disk
        assert not os.path.exists(local_file)