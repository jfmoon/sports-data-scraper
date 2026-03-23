import requests
import os
from datetime import datetime, timezone
from base.scraper import BaseScraper
from base.models import TennisOdds
from base.storage import StorageManager

class TheOddsApiScraper(BaseScraper):
    def fetch(self):
        api_key = os.environ.get("THE_ODDS_API_KEY")
        if not api_key:
            raise ValueError("THE_ODDS_API_KEY not set")
        url = f"https://api.the-odds-api.com/v4/sports/tennis/odds/?apiKey={api_key}&regions=us&markets=h2h"
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        return res.json()

    def content_key(self, raw):
        return raw

    def parse(self, raw):
        odds_out = []
        for event in raw:
            book = next((b for b in event.get("bookmakers", []) if b["key"] == "draftkings"), None)
            if not book: continue

            h2h = next((m for m in book["markets"] if m["key"] == "h2h"), None)
            if not h2h: continue

            odds_out.append({
                "match_id": event["id"],
                "tournament": event["sport_title"],
                "p1_name": self.resolver.resolve(event["home_team"]),
                "p2_name": self.resolver.resolve(event["away_team"]),
                "p1_ml": next((o["price"] for o in h2h["outcomes"] if o["name"] == event["home_team"]), None),
                "p2_ml": next((o["price"] for o in h2h["outcomes"] if o["name"] == event["away_team"]), None),
                "bookmaker": "DraftKings",
                "commence_time": event["commence_time"]
            })
        return odds_out

    def validate(self, records):
        return [TennisOdds(**r) for r in records]

    def upsert(self, records):
        storage = StorageManager(self.config["bucket"])
        payload = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "odds": [r.model_dump() for r in records]
        }
        storage.write_json(self.config["gcs_object"], payload)