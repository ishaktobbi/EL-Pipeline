import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from tqdm import tqdm

from datetime import datetime
# from pathlib import Path
# from dotenv import load_dotenv

class GCSDataPipeline:
    def __init__(self, project_id: str, bucket_name: str):
        self.project_id = project_id
        self.bucket_name = bucket_name
        
        # Connects using your active authenticated gcloud session
        self.storage_client = storage.Client(project=self.project_id)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    def extract_and_convert_stream(self,execution_date: str , taxi_color: str):
        

        # Set up your explicit schema schemas
        # dtype = {
        #         "VendorID": "Int64",
        #         "passenger_count": "Int64",
        #         "trip_distance": "float64",
        #         "RatecodeID": "Int64",
        #         "store_and_fwd_flag": "string",
        #         "PULocationID": "Int64",
        #         "DOLocationID": "Int64",
        #         "payment_type": "Int64",
        #         "fare_amount": "float64",
        #         "extra": "float64",
        #         "mta_tax": "float64",
        #         "tip_amount": "float64",
        #         "tolls_amount": "float64",
        #         "improvement_surcharge": "float64",
        #         "total_amount": "float64",
        #         "congestion_surcharge": "float64"
        # }

        # parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"] if taxi_color == "yellow" else ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

        date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_color}/{taxi_color}_tripdata_{year}-{month}.csv.gz"

        print(f"📡 Stream-ingesting and converting data from: {url}")

        local_parquet_path = f"{taxi_color}_tripdata_{year}-{month}.parquet"

        # Step 1: Open a streaming HTTP network pipe (No full file download!)
        print("📥 Downloading compressed data...")
        with requests.get(url, stream=True, timeout=60) as response:
            
            # 🟢 INDUSTRY SAFEGUARD: Handle missing files gracefully
            if response.status_code == 404:
                print(f"⚠️ Warning: File not found for {year}-{month} (404). It might not be published yet.")
                # Return None so the downstream load step knows to do nothing
                return None
            
            response.raise_for_status()     # raise an error if the request failed (e.g., 404, 500, etc.)

    
            # Step 2: Pass the raw network stream directly into pandas using chunksize
            # This returns an iterator engine instead of loading a static DataFrame (act as a valve that waits for data to be extracted when called in an iteration)
            csv_stream_iterator = pd.read_csv(
                response.raw,  # Directly reads raw bytes from the network pipe
                dtype=str,  # Read all columns as strings to avoid dtype inference issues
                # parse_dates=parse_dates,
                chunksize=100000,  # Process exactly 100,000 rows at a time in RAM / we are safe from getting half row, so we can parse by a chunk safely.
                low_memory=False,
                compression='gzip'  # Assuming the CSV is compressed with gzip; adjust if needed
            )


            parquet_writer = None

            # Step 3: Loop through the file block-by-block (here the read_csv start reading data from the network)
            for chunk_df in tqdm(csv_stream_iterator):

                # print(f"check data while processing: {chunk_df.isnull().sum()}")  # Optional: Print summary statistics for each chunk (for debugging)

                # Convert the small pandas chunk into a PyArrow Table block
                arrow_table = pa.Table.from_pandas(chunk_df)

                # Initialize the Parquet file schema on disk using the first chunk (then waiting for data to append)
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(local_parquet_path, arrow_table.schema, compression='snappy')

                # Append the chunk directly to the final Parquet file
                parquet_writer.write_table(arrow_table)

            # Close the file pointer safely
            if parquet_writer:
                parquet_writer.close()

        print(f"🚀 Perfect streaming extraction complete. Saved directly to: {local_parquet_path}")
        return local_parquet_path

    def load_to_gcs(self, local_file_path: str, execution_date: str, taxi_color: str):
        """
        Streams data into a strictly partitioned folder structure.
        execution_date format: '2026-06-30'
        """


        # 1. Parse the date string to extract year, month, and day chunks
        date_obj = datetime.strptime(execution_date, "%Y-%m-%d")    # Y is Maj, not like m and d
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        # day = date_obj.strftime("%d")

        if local_file_path == None:
            print(f"data not found to load for the date {year}-{month} !")
            return

        # 2. Build Hive-style partitioning paths (the gold standard in data lakes)
        # Result path: raw/taxi/year=2026/month=06/day=30/data.parquet
        destination_blob_name = f"raw/taxi/{taxi_color}/year={year}/month={month}/data.parquet"

        print(f"🚀 Uploading to GCS Data Lake as: '{destination_blob_name}'...")

        blob = self.bucket.blob(destination_blob_name)

        # check if blob already exists, if yes, delete it to ensure idempotency
        if blob.exists():
            print(f"⚠️ Blob already exists at gs://{self.bucket_name}/{destination_blob_name}. Overwriting for idempotency...")
        
        # 3. Stream upload (Overwrites if it exists, ensuring idempotency)
        blob.upload_from_filename(local_file_path)

        
        os.remove(local_file_path)

        print(f"✅ Idempotent upload finished: gs://{self.bucket_name}/{destination_blob_name}")

if __name__ == "__main__":

    # EXTRACTED_PARQUET_FILE = "data_extracted.parquet"
    # DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    # -> Point to the .env file 
    # ENV_PATH = Path(__file__).resolve().parent / "../orchestrator/.env"
    # -> Load the .env file to the environment
    # load_dotenv(dotenv_path=ENV_PATH)

    # -> fetch configurations from the environment
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
    EXECUTION_DATE = os.environ.get("EXECUTION_DATE")
    TAXI_COLOR = os.environ.get("TAXI_COLOR")

    
    # -> Validation step to ensure your container crashes early if you forgot to set them
    if not PROJECT_ID or not BUCKET_NAME or not EXECUTION_DATE or not TAXI_COLOR:
        raise ValueError("❌ Missing required environment variables: GCP_PROJECT_ID, GCS_BUCKET_NAME, EXECUTION_DATE, or TAXI_COLOR")

    # -> Execute Pipeline
    pipeline = GCSDataPipeline(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    
    try:
        print(f"🚀 Starting {PROJECT_ID} pipeline for {TAXI_COLOR} taxi data on {EXECUTION_DATE}...")
        local_file = pipeline.extract_and_convert_stream(EXECUTION_DATE, TAXI_COLOR)
        print(f"local file name : {local_file}")
        pipeline.load_to_gcs(local_file, EXECUTION_DATE, TAXI_COLOR)
    except Exception as e:
        print(f"💥 Pipeline execution failed: {e}")