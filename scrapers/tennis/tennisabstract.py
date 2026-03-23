"""
Tennis Abstract scraper wrapper for the sports-data-scraper framework.

Delegates all scraping to tennisabstract_scraper.py (Playwright + curl_cffi).
Scrapes WTA player profiles, career stats, Elo ratings, recent match results,
and computes 1-10 attribute scores for the WTA Style Classifier.

After the scrape completes it:
  1. Uploads raw JSON  → GCS: raw/tennisabstract/{date}/players_{ts}.json
  2. Writes parsed     → GCS: tennis/players.json

Config keys (in config.yaml under tennisabstract):
  top: 100                # number of top-ranked players to scrape
  slug: null              # single player slug for testing (e.g. IgaSwiatek)
  priority_players:       # always scraped regardless of ranking
    - {name: "Emma Raducanu", slug: "EmmaRaducanu"}
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from base.scraper import BaseScraper
from base.storage import StorageManager


TENNISABSTRACT_SCRAPER = os.path.join(
    os.path.dirname(__file__), "tennisabstract_scraper.py"
)


class TennisAbstractScraper(BaseScraper):

    def fetch(self):
        top              = self.config.get("top", 100)
        slug             = self.config.get("slug", None)
        priority_players = self.config.get("priority_players", [])
        date_str         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str           = datetime.now(timezone.utc).strftime("%H%M%S")
        out_dir          = f"data/raw/tennisabstract/{date_str}"
        os.makedirs(out_dir, exist_ok=True)

        env = os.environ.copy()
        env["GCS_BUCKET"] = self.config.get("bucket", "")

        all_results = []
        errors      = []

        # ── Single-player test mode ───────────────────────────────────────────
        if slug:
            out_path = f"{out_dir}/players_{ts_str}.json"
            env["OUTPUT_PATH"] = out_path
            env["TOP_N"]       = "0"
            print(f"\n[TennisAbstract] Single player mode: {slug}")
            subprocess.run([sys.executable, TENNISABSTRACT_SCRAPER, slug],
                           check=True, env=env)
            with open(out_path, "r") as f:
                data = json.load(f)
            self._raw_path = out_path
            self._raw_mode = "players"
            return {"mode": "players", "path": out_path, "data": data}

        # ── Normal mode: top N rankings ───────────────────────────────────────
        ranked_path = f"{out_dir}/ranked_{ts_str}.json"
        env["OUTPUT_PATH"] = ranked_path
        env["TOP_N"]       = str(top)
        print(f"\n[TennisAbstract] Scraping top {top} WTA players...")
        subprocess.run([sys.executable, TENNISABSTRACT_SCRAPER],
                       check=True, env=env)

        with open(ranked_path, "r") as f:
            ranked_data = json.load(f)

        ranked_slugs = {p["slug"] for p in ranked_data.get("players", [])}
        all_results  = ranked_data.get("players", [])
        errors       = ranked_data.get("errors", [])

        # ── Priority players not already in top N ─────────────────────────────
        extras = [p for p in priority_players if p["slug"] not in ranked_slugs]
        if extras:
            print(f"\n[TennisAbstract] Scraping {len(extras)} priority players not in top {top}...")
            for i, player in enumerate(extras):
                extra_path = f"{out_dir}/priority_{player['slug']}_{ts_str}.json"
                env["OUTPUT_PATH"] = extra_path
                env["TOP_N"]       = "0"
                try:
                    subprocess.run(
                        [sys.executable, TENNISABSTRACT_SCRAPER, player["slug"]],
                        check=True, env=env
                    )
                    with open(extra_path, "r") as f:
                        extra_data = json.load(f)
                    players = extra_data.get("players", [])
                    if players:
                        all_results.extend(players)
                        print(f"  ✅ {player['name']}")
                    else:
                        errors.append(player["name"])
                except Exception as e:
                    print(f"  ❌ Failed {player['name']}: {e}")
                    errors.append(player["name"])

        # ── Write merged output file ──────────────────────────────────────────
        merged_path = f"{out_dir}/players_{ts_str}.json"
        merged = {
            "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "playerCount": len(all_results),
            "players":     all_results,
            "errors":      errors,
        }
        with open(merged_path, "w") as f:
            json.dump(merged, f, indent=2)

        self._raw_path = merged_path
        self._raw_mode = "players"

        return {"mode": "players", "path": merged_path, "data": merged}

    def content_key(self, raw):
        d = raw.get("data", {})
        return {
            "lastUpdated": d.get("lastUpdated"),
            "playerCount": d.get("playerCount"),
        }

    def parse(self, raw):
        return raw["data"].get("players", [])

    def validate(self, records):
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
