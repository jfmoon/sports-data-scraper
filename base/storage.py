import json
import os
from datetime import datetime
from google.cloud import storage


class StorageManager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client()

    def persist_raw(self, source, data):
        """
        Dual-write raw data:
          1. Local disk → data/raw/{source}/{date}/{timestamp}.json
          2. GCS        → raw/{source}/{date}/{timestamp}.json
        """
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        ts_str   = datetime.utcnow().strftime("%H%M%S")

        # Local
        local_path = f"data/raw/{source}/{date_str}"
        os.makedirs(local_path, exist_ok=True)
        with open(f"{local_path}/{ts_str}.json", "w") as f:
            json.dump(data, f, indent=2)

        # GCS
        try:
            blob = self.client.bucket(self.bucket_name).blob(
                f"raw/{source}/{date_str}/{ts_str}.json"
            )
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type="application/json"
            )
        except Exception as e:
            print(f"  [warn] GCS raw upload failed for {source}: {e}")

    def write_json(self, blob_name, data):
        """Write parsed output to GCS."""
        blob = self.client.bucket(self.bucket_name).blob(blob_name)
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json"
        )
        return f"gs://{self.bucket_name}/{blob_name}"

    def write_raw_file(self, gcs_object, local_path):
        """Upload a local file (e.g. CSV) to GCS verbatim."""
        blob = self.client.bucket(self.bucket_name).blob(gcs_object)
        blob.upload_from_filename(local_path)
        return f"gs://{self.bucket_name}/{gcs_object}"
