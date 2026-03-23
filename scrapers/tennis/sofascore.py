import requests
from datetime import datetime, timezone
from base.scraper import BaseScraper
from base.models import TennisMatch
from base.storage import StorageManager

class SofaScoreScraper(BaseScraper):
    def fetch(self):
        from datetime import date
        today = date.today().strftime("%Y-%m-%d")
        url = f"https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/{today}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://www.sofascore.com/",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.sofascore.com",
        }
        res = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        return res.json()

    def content_key(self, raw):
        return raw.get("events", [])

    def parse(self, raw):
        matches = []
        for ev in raw.get("events", []):
            if ev.get("tournament", {}).get("category", {}).get("name") != "WTA":
                continue
            matches.append({
                "match_id":    str(ev["id"]),
                "tournament":  ev["tournament"]["name"],
                "status":      "live" if ev["status"]["type"] == "inprogress" else "scheduled",
                "p1_name":     self.resolver.resolve(ev["homeTeam"]["name"]),
                "p2_name":     self.resolver.resolve(ev["awayTeam"]["name"]),
                "p1_sets_won": ev.get("homeScore", {}).get("display"),
                "p2_sets_won": ev.get("awayScore", {}).get("display"),
                "date":        datetime.fromtimestamp(ev["startTimestamp"]).strftime("%Y-%m-%d"),
                "set_scores":  []
            })
        return matches

    def validate(self, records):
        return [TennisMatch(**r) for r in records]

    def upsert(self, records):
        storage = StorageManager(self.config["bucket"])
        payload = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "matches": [r.model_dump() for r in records]
        }
        storage.write_json(self.config["gcs_object"], payload)
