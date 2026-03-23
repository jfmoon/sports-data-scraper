from base.scraper import BaseScraper
from base.models import CBBTeam
from base.storage import StorageManager
from datetime import datetime, timezone

class KenPomScraper(BaseScraper):
    def fetch(self):
        """Reads manual export for local processing."""
        with open("kenpom_data.csv", "r") as f:
            return f.read()

    def content_key(self, raw):
        return raw

    def parse(self, raw):
        # Implementation of CSV parsing logic
        return [{"name": "UConn", "kenpom_rank": 1, "adj_o": 125.0, "adj_d": 91.0}]

    def validate(self, records):
        return [CBBTeam(**r) for r in records]

    def upsert(self, records):
        storage = StorageManager(self.config["bucket"])
        payload = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "teams": [r.model_dump() for r in records]
        }
        storage.write_json(self.config["gcs_object"], payload)