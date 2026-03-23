import json
from datetime import datetime, timezone
from google.cloud import storage

class StateManager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.state_file = "scrape_state.json"

    def get_state(self, source):
        try:
            blob = self.client.bucket(self.bucket_name).blob(self.state_file)
            return json.loads(blob.download_as_text()).get(source, {})
        except Exception:
            return {}

    def update_success(self, source, new_hash):
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(self.state_file)
        try:
            full_state = json.loads(blob.download_as_text())
        except Exception:
            full_state = {}
        
        full_state[source] = {
            "last_success_at": datetime.now(timezone.utc).isoformat(),
            "last_content_hash": new_hash,
            "status": "ok",
            "error_count": 0
        }
        blob.upload_from_string(json.dumps(full_state, indent=2))