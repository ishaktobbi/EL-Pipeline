import os
import urllib.request
import pandas as pd
from google.cloud import storage

class GCSDataPipeline:
    def __init__(self, project_id: str, bucket_name: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        
        # Connects using your active authenticated gcloud session
        self.storage_client = storage.Client(project=self.project_id)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    def extract_and_convert(self, url: str,target_compressed_csv: str, local_parquet_path: str) -> str:
        """Downloads a CSV dataset from the web and converts it to Parquet."""
        
        print(f"📡 Downloading raw data from: {url}")
        
        urllib.request.urlretrieve(url, target_compressed_csv)
        

        print("⚡ Converting raw CSV to optimized Parquet format...")
        # Read the file and parse it into an optimized columnar Parquet format
        df = pd.read_csv(target_compressed_csv, low_memory=False)
        df.to_parquet(local_parquet_path, engine="pyarrow", compression="snappy")
        
        # Delete temporary raw CSV to save local space
        
        os.remove(target_compressed_csv)
        

        return local_parquet_path

    def load_to_lake(self, local_file_path: str, destination_blob_name: str):
        """Streams the compressed analytical file straight into GCS."""
        print(f"🚀 Uploading to GCS Data Lake as: '{destination_blob_name}'...")
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file_path)
        print("✅ Load phase complete! Cloud asset is live.")

        os.remove(local_file_path) # Clean up local file after upload

if __name__ == "__main__":
    # 🌟 Dynamically fetch configurations from the environment
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    
    # Validation step to ensure your container crashes early if you forgot to set them
    if not PROJECT_ID or not BUCKET_NAME:
        raise ValueError("❌ Missing required environment variables: GCP_PROJECT_ID or GCS_BUCKET_NAME")

    DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    TARGET_COMPRESSED_CSV = "temp_dataset.csv.gz"
    TARGET_PARQUET = "yellow_tripdata_2021-01.parquet"
    BLOB_PATH = "raw/ny_taxi/yellow_tripdata_2021-01.parquet"

    # Execute Pipeline
    pipeline = GCSDataPipeline(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    
    try:
        local_file = pipeline.extract_and_convert(DATA_URL, TARGET_COMPRESSED_CSV, TARGET_PARQUET)
        pipeline.load_to_lake(local_file, BLOB_PATH)
    except Exception as e:
        print(f"💥 Pipeline execution failed: {e}")