import json
import os
from datetime import datetime
from google.cloud import storage

class StorageManager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client()

    def persist_raw(self, source, data):
        """Audit trail: save raw JSON to ephemeral disk."""
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        ts_str = datetime.utcnow().strftime("%H%M%S")
        path = f"data/raw/{source}/{date_str}"
        os.makedirs(path, exist_ok=True)
        filename = f"{path}/{ts_str}.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

    def write_json(self, blob_name, data):
        """Source of Truth: write to GCS and make public."""
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, indent=2), 
            content_type="application/json"
        )
        blob.make_public()
        return blob.public_url