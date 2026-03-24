import requests
from datetime import datetime, timezone
from base.scraper import BaseScraper
from base.models import CBBGame
from base.storage import StorageManager
from scrapers.cbb.names import to_canonical


class ESPNScraper(BaseScraper):
    def fetch(self):
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        return res.json()

    def content_key(self, raw):
        return raw.get("events", [])

    def parse(self, raw):
        results = []
        for ev in raw.get("events", []):
            try:
                comp   = ev["competitions"][0]
                t1, t2 = comp["competitors"][0], comp["competitors"][1]

                def clean_score(s):
                    if s is None or s == "": return None
                    try: return int(s)
                    except: return None

                results.append({
                    "espn_id":   ev["id"],
                    "date":      ev["date"][:10],
                    "state":     ev["status"]["type"]["state"],
                    "completed": ev["status"]["type"]["completed"],
                    "t1_name":   to_canonical(self.resolver.resolve(t1["team"]["displayName"])),
                    "t1_score":  clean_score(t1.get("score")),
                    "t1_winner": t1.get("winner", False),
                    "t2_name":   to_canonical(self.resolver.resolve(t2["team"]["displayName"])),
                    "t2_score":  clean_score(t2.get("score")),
                    "t2_winner": t2.get("winner", False),
                })
            except Exception as e:
                print(f"Error parsing ESPN event {ev.get('id')}: {e}")
        return results

    def validate(self, records):
        return [CBBGame(**r) for r in records]

    def upsert(self, records):
        storage = StorageManager(self.config["bucket"])
        payload = {
            "updated":    datetime.now(timezone.utc).isoformat(),
            "game_count": len(records),
            "games":      [r.model_dump(mode="json") for r in records]
        }
        storage.write_json(self.config["gcs_object"], payload)
