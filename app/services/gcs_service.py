import json
import uuid
from datetime import datetime
from google.cloud import storage
import os

class GCSService:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = storage.Client(project=os.getenv("GCP_PUB_SUB_PROJECT_ID"))
        self.bucket = self.client.bucket(bucket_name)

    def upload_json(self, data: dict, prefix: str = "errors") -> str:
        
        """
        Uploads JSON data to GCS under the given prefix with a unique filename.
        Returns the GCS URI of the uploaded file.
        """
        date_path = datetime.utcnow().strftime("%Y/%m/%d")
        job_id = str(uuid.uuid4())
        filename = f"{job_id}.json"
        blob_path = f"{prefix}/{date_path}/{filename}"
        blob = self.bucket.blob(blob_path)
        json_data = json.dumps(data, indent=2)
        blob.upload_from_string(json_data, content_type="application/json")
        return {
            "uri":f"https://storage.googleapis.com/{self.bucket_name}/{blob_path}",
            "filename":job_id
        }
