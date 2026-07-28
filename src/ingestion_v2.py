import os
import re
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, ReadTimeout

from urllib3.util.retry import Retry
from google.cloud import storage
from google.cloud.exceptions import NotFound, Forbidden
from tqdm import tqdm

from src.utils import parse_date, validate_color

# from airflow.exceptions import AirflowSkipException

# Handle running standalone vs. running inside Airflow environment
try:
    from airflow.exceptions import AirflowSkipException
except ImportError:
    class AirflowSkipException(Exception):
        """Fallback custom exception when running script outside Airflow."""
        pass

class NYCDataIngestor:
    """Handles streaming extract and local parquet conversion of NYC Taxi data."""


    def __init__(self, execution_date: str, taxi_color: str, chunksize: int = 100000):
        # 1. Validate inputs immediately on init
        if chunksize <= 0:
            raise ValueError(f"❌ Invalid chunksize '{chunksize}'. Must be > 0.")
        
        self.chunksize = chunksize
        self.taxi_color = validate_color(taxi_color)
        self.year, self.month = parse_date(execution_date)

        # 2. Assign core derived attributes early
        self.url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
            f"{self.taxi_color}/{self.taxi_color}_tripdata_{self.year}-{self.month}.csv.gz"
        )
        self.local_parquet_path = f"{self.taxi_color}_tripdata_{self.year}-{self.month}.parquet"
        
        self.session = self._create_retry_session()

    # @staticmethod   # the function can be called in different classes with name.func
    # def validate_color(taxi_color: str) -> str:
    #     color = taxi_color.lower().strip()
    #     if color not in NYCDataIngestor.VALID_TAXI_COLORS:
    #         raise ValueError(f"❌ Invalid taxi color '{taxi_color}'. Allowed: {NYCDataIngestor.VALID_TAXI_COLORS}")
    #     return color

    # @staticmethod
    # def parse_date(execution_date: str) -> tuple[str, str]:
    #     if not re.match(r"^\d{4}-\d{2}-\d{2}$", execution_date):
    #         raise ValueError(f"❌ Invalid date format: '{execution_date}'. Expected 'YYYY-MM-DD'.")
    #     try:
    #         date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    #         return date_obj.strftime("%Y"), date_obj.strftime("%m")
    #     except ValueError as e:
    #         raise ValueError(f"❌ Invalid calendar date '{execution_date}': {e}") from e

    @staticmethod
    def _create_retry_session(retries: int = 3, backoff_factor: float = 2.0) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    # def extract_and_convert(self) -> str:
    #     """Streams raw compressed CSV from self.url and writes to self.local_parquet_path."""
    #     print(f"📡 Stream-ingesting from: {self.url}")

    def extract_and_convert(self) -> str:
        """ - 1 - Main orchestrator: fetches stream and triggers stream processing."""
        """Streams raw compressed CSV from self.url and writes to self.local_parquet_path."""
        print(f"📡 Stream-ingesting from: {self.url}")
        success = False
        try:
            self._fetch_stream()
            # self._process_csv_stream_to_parquet(response.raw)
            print(f"🚀 Saved Parquet locally: {self.local_parquet_path}")
            success = True
            return self.local_parquet_path

        except AirflowSkipException:
            # Let Airflow handle skip exceptions natively
            raise
        except ConnectTimeout:
                raise RuntimeError("Connection timed out")
        except ReadTimeout:
                raise RuntimeError("Connection timed out middle way")
        
        except Exception as e:
            raise RuntimeError(f"💥 Failed processing {self.taxi_color} data for {self.year}-{self.month}: {e}")
        finally:
            # Cleanup partial/corrupt file ONLY if execution failed!
            if not success and os.path.exists(self.local_parquet_path):
                os.remove(self.local_parquet_path)

    def _fetch_stream(self):
        """- 2 - Handles HTTP connection and 404/status check."""
        with self.session.get(self.url, stream=True, timeout=(10, 60)) as response:
            
            if response.status_code == 404:
                print(f"⚠️ File not found for {self.year}-{self.month} (404).")
                raise AirflowSkipException(f"No source data published for {self.year}-{self.month}")

            response.raise_for_status()
            # return response

            self._process_csv_stream_to_parquet(response.raw)
        

    def _process_csv_stream_to_parquet(self, stream_source) -> None:
        """- 3 - Handles CSV streaming and Parquet conversion with writer cleanup."""
        print("reading file with pandas starting")
        try:
            csv_stream_iterator = pd.read_csv(
                stream_source,
                dtype=str,
                chunksize=self.chunksize,
                low_memory=False,
                compression="gzip",
            )
            self._write_chunks_to_parquet(csv_stream_iterator)

        except pd.errors.EmptyDataError:
            print(f"⚠️ File at {self.url} is empty.")
            raise AirflowSkipException(f"Empty file encountered for {self.year}-{self.month}")
            

    def _write_chunks_to_parquet(self, csv_stream_iterator) -> None:
        """- 4 - Writes dataframe chunks to a Parquet file using context/finally guarantee."""
        parquet_writer = None
        rows_written = 0
        try:
            for chunk_df in tqdm(csv_stream_iterator, desc="Processing Chunks"):
                if not chunk_df.empty:
                    rows_written += 1

                    arrow_table = pa.Table.from_pandas(chunk_df)

                    if parquet_writer is None:
                        parquet_writer = pq.ParquetWriter(
                            self.local_parquet_path,
                            arrow_table.schema,
                            compression="snappy",
                        )

                    parquet_writer.write_table(arrow_table)

            # 🔑 Handle empty CSVs that have headers but 0 data rows
            if rows_written == 0:
                raise AirflowSkipException(
                    f"Empty dataset: No data rows found for {self.year}-{self.month}"
                )
        finally:
            if parquet_writer:
                parquet_writer.close()


class GCSUploader:
    """Handles idempotent uploads to Google Cloud Storage with bucket validation."""

    def __init__(self, project_id: str, bucket_name: str, execution_date: str, taxi_color: str, client: storage.Client = None):
        # 1. Validate environment strings

        self.project_id = self._validate_non_empty_string(project_id, "project_id")
        self.bucket_name = self._validate_non_empty_string(bucket_name, "bucket_name")
        
        self.taxi_color = validate_color(taxi_color)

        self.year, self.month = parse_date(execution_date)

        self.destination_blob_name = (
            f"raw/taxi/{self.taxi_color}/year={self.year}/month={self.month}/data.parquet"
        )

        # 3. Authenticate & Verify Bucket Access
        # Passing client allows easily injecting a mock storage client during unit testing
        self.storage_client = client or storage.Client(project=self.project_id)
        self.bucket = self._verify_bucket()

    @staticmethod
    def _validate_non_empty_string(value: str, param_name: str) -> str:
        """Helper method to ensure a parameter is a non-empty string."""
        if not isinstance(value, str):
            raise TypeError(
                f"❌ {param_name} must be a string, got {type(value).__name__}."
            )
        
        cleaned_value = value.strip()
        if not cleaned_value:
            raise ValueError(f"❌ {param_name} cannot be empty.")
        
        return cleaned_value

    def _verify_bucket(self):
        """Validates bucket existence and permissions upon class initialization."""
        
        try:
            return self.storage_client.get_bucket(self.bucket_name)
        except NotFound:
            raise ValueError(
                f"❌ Bucket '{self.bucket_name}' (or project '{self.project_id}') does not exist."
            )
        except Forbidden:
            raise PermissionError(
                f"❌ Access denied to project/bucket. Check Service Account IAM roles for '{self.project_id}'."
            )
        except Exception as e:
            raise RuntimeError(f"💥 Failed to access GCS bucket '{self.bucket_name}' with GCP project id '{self.project_id}' : {e}") from e

    def upload_file(self, local_file_path: str) -> None:
        """Uploads a local Parquet file to Hive-partitioned GCS path and guarantees local cleanup."""
        if not local_file_path or not os.path.exists(local_file_path):
            raise FileNotFoundError(f"❌ Local file not found for upload: '{local_file_path}'")

        target_blob = self.bucket.blob(self.destination_blob_name)

        if target_blob.exists():
            print(f"⚠️ Blob gs://{self.bucket_name}/{self.destination_blob_name} exists. Overwriting...")

        try:
            print(f"🚀 Uploading to GCS: gs://{self.bucket_name}/{self.destination_blob_name}")
            target_blob.upload_from_filename(local_file_path)
            print("✅ Upload complete.")
        except :
            raise Exception("GCS Connection Interrupted")
        finally:
            # 🟢 CRITICAL GUARANTEE: Remove the local file even if target_blob.upload_from_filename raises an exception!
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                print(f"🧹 Local file removed: {local_file_path}")


if __name__ == "__main__":
    # 1. Fetch environment variables
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    EXECUTION_DATE = os.environ.get("EXECUTION_DATE")
    TAXI_COLOR = os.environ.get("TAXI_COLOR")

    # 2. Crash fast if any required variables are missing
    if not all([PROJECT_ID, BUCKET_NAME, EXECUTION_DATE, TAXI_COLOR]):
        missing = [
            var
            for var, val in [
                ("GCP_PROJECT_ID", PROJECT_ID),
                ("GCS_BUCKET_NAME", BUCKET_NAME),
                ("EXECUTION_DATE", EXECUTION_DATE),
                ("TAXI_COLOR", TAXI_COLOR),
            ]
            if not val
        ]
        raise ValueError(f"❌ Missing required environment variables: {missing}")

    # 3. Instantiate objects (Inputs get validated immediately on init)
    ingestor = NYCDataIngestor(execution_date=EXECUTION_DATE, taxi_color=TAXI_COLOR)
    uploader = GCSUploader(
        project_id=PROJECT_ID,
        bucket_name=BUCKET_NAME,
        execution_date=EXECUTION_DATE,
        taxi_color=TAXI_COLOR,
    )

    # 4. Pipeline Execution Pipeline
    try:
        print(f"🚀 Pipeline starting for {TAXI_COLOR} taxi data on {EXECUTION_DATE}...")
        parquet_file = ingestor.extract_and_convert()
        uploader.upload_file(parquet_file)
    except AirflowSkipException as e:
        print(f"🌸 Task skipped gracefully: {e}")
        raise e
    except Exception as e:
        print(f"💥 Pipeline execution failed: {e}")
        raise e