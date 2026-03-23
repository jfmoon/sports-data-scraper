"""
Tennis Abstract scraper wrapper for the sports-data-scraper framework.

Delegates all scraping to tennisabstract_scraper.py (Playwright + curl_cffi).
Scrapes WTA player profiles, career stats, Elo ratings, recent match results,
and computes 1-10 attribute scores for the WTA Style Classifier.

After the scrape completes it:
  1. Uploads raw JSON  → GCS: raw/tennisabstract/{date}/players_{ts}.json
  2. Writes parsed     → GCS: tennis/players.json  (canonical player data)

Config keys (in config.yaml under tennisabstract):
  top: 100          # number of top-ranked players to scrape
  slug: null        # single player slug for testing (e.g. IgaSwiatek)
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from base.scraper import BaseScraper
from base.models import TennisPlayer
from base.storage import StorageManager


TENNISABSTRACT_SCRAPER = os.path.join(
    os.path.dirname(__file__), "tennisabstract_scraper.py"
)


class TennisAbstractScraper(BaseScraper):

    def fetch(self):
        top      = self.config.get("top", 100)
        slug     = self.config.get("slug", None)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str   = datetime.now(timezone.utc).strftime("%H%M%S")
        out_dir  = f"data/raw/tennisabstract/{date_str}"
        os.makedirs(out_dir, exist_ok=True)

        out_path = f"{out_dir}/players_{ts_str}.json"

        env = os.environ.copy()
        env["OUTPUT_PATH"]  = out_path
        env["TOP_N"]        = str(top)
        # Point scraper at our bucket so its internal GCS upload also works
        env["GCS_BUCKET"]   = self.config.get("bucket", "")

        cmd = [sys.executable, TENNISABSTRACT_SCRAPER]
        if slug:
            cmd.append(slug)

        print(f"\n[TennisAbstract] Scraping top {top} WTA players → {out_path}")
        subprocess.run(cmd, check=True, env=env)

        with open(out_path, "r") as f:
            data = json.load(f)

        self._raw_path = out_path
        self._raw_mode = "players"

        return {"mode": "players", "path": out_path, "data": data}

    def content_key(self, raw):
        # Hash on lastUpdated + playerCount — full data is too large to hash every run
        d = raw.get("data", {})
        return {
            "lastUpdated": d.get("lastUpdated"),
            "playerCount": d.get("playerCount"),
        }

    def parse(self, raw):
        return raw["data"].get("players", [])

    def validate(self, records):
        # Player records are rich dicts — pass through without stripping to Pydantic
        # TennisPlayer model is a subset; full records include ratings, elo, matches
        return records

    def upsert(self, records):
        storage  = StorageManager(self.config["bucket"])
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str   = datetime.now(timezone.utc).strftime("%H%M%S")

        raw_path = getattr(self, "_raw_path", None)
        if raw_path and os.path.exists(raw_path):
            gcs_raw = f"raw/tennisabstract/{date_str}/players_{ts_str}.json"
            storage.write_raw_file(gcs_raw, raw_path)
            print(f"  [TennisAbstract] Raw → gs://{self.config['bucket']}/{gcs_raw}")

        payload = {
            "updated":      datetime.now(timezone.utc).isoformat(),
            "player_count": len(records),
            "players":      records,
        }

        url = storage.write_json(self.config["gcs_object"], payload)
        print(f"  [TennisAbstract] Parsed output → {url}")
