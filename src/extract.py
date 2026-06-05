import os
from google.cloud import storage

class DataLakeClient:
    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name
        
        # Explicitly passing the project keeps the connection robust!
        self.storage_client = storage.Client(project=self.project_id)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

    def upload_to_lake(self, local_file_path, destination_blob_name):
        """Uploads a local file to the GCS Data Lake bucket."""
        print(f"🚀 Uploading {local_file_path} to lake as '{destination_blob_name}'...")
        
        # Create a pointer to the target file path inside the bucket
        blob = self.bucket.blob(destination_blob_name)
        
        # Stream the file up to the cloud
        blob.upload_from_filename(local_file_path)
        print(f"✅ Upload complete! File is now safely in your Data Lake.")

if __name__ == "__main__":
    # 🏁 Set your identifiers
    PROJECT_ID = "elpipeline"
    BUCKET_NAME = "ishak-tobbi-data-lake-2026"
    
    # Initialize our custom pipeline worker
    lake = DataLakeClient(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)
    
    # Let's create a tiny dummy data file right now to test a real upload!
    test_file = "sample_data.txt"
    with open(test_file, "w") as f:
        f.write("id,name,role\n1,Alex,Data Engineer\n2,Sam,ML Engineer")
        
    # Trigger the upload into a folder called 'raw/' inside your bucket
    lake.upload_to_lake(local_file_path=test_file, destination_blob_name="raw/sample_data.txt")
    
    # Clean up the local test file from your Mac
    if os.path.exists(test_file):
        os.remove(test_file)