"""
KenPom scraper wrapper for the sports-data-scraper framework.

Delegates all browser automation to kenpom_scraper.py (Playwright + stealth).
After the scrape completes it:
  1. Uploads raw CSV   → GCS: raw/kenpom/{date}/kenpom_{ts}.csv
  2. Uploads raw JSON  → GCS: raw/kenpom/{date}/fanmatch_{ts}.json  (fanmatch mode)
  3. Writes parsed     → GCS: cbb/kenpom.json
  4. Saves local copy  → data/raw/kenpom/{date}/

Config keys (in config.yaml under kenpom):
  top: 68               # number of teams to scrape
  fanmatch: false       # set true to scrape FanMatch predictions instead
  fanmatch_date: null   # "YYYY-MM-DD" or null for today
"""

import csv
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

from base.scraper import BaseScraper
from base.models import CBBTeam
from base.storage import StorageManager


# kenpom_scraper.py lives alongside this file in scrapers/cbb/
KENPOM_SCRAPER = os.path.join(os.path.dirname(__file__), "kenpom_scraper.py")


class KenPomScraper(BaseScraper):

    def fetch(self):
        top         = self.config.get("top", 68)
        fanmatch    = self.config.get("fanmatch", False)
        fanmatch_dt = self.config.get("fanmatch_date", None)
        date_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str      = datetime.now(timezone.utc).strftime("%H%M%S")
        out_dir     = f"data/raw/kenpom/{date_str}"
        os.makedirs(out_dir, exist_ok=True)

        if fanmatch:
            out_path = f"{out_dir}/fanmatch_{ts_str}.json"
            cmd = [sys.executable, KENPOM_SCRAPER,
                   "--fanmatch", "--fanmatch-out", out_path]
            if fanmatch_dt:
                cmd += ["--fanmatch-date", fanmatch_dt]
            print(f"\n[KenPom] FanMatch mode → {out_path}")
        else:
            out_path = f"{out_dir}/kenpom_{ts_str}.csv"
            cmd = [sys.executable, KENPOM_SCRAPER,
                   "--top", str(top), "--out", out_path]
            print(f"\n[KenPom] Ratings scrape → {out_path}")

        print("[KenPom] Browser opening — log in when prompted.\n")
        subprocess.run(cmd, check=True)

        if fanmatch:
            with open(out_path, "r") as f:
                data = json.load(f)
        else:
            with open(out_path, "r") as f:
                data = f.read()

        # Stash for upsert()
        self._raw_path = out_path
        self._raw_mode = "fanmatch" if fanmatch else "ratings"

        return {"mode": self._raw_mode, "path": out_path, "data": data}

    def content_key(self, raw):
        return raw.get("data", "")

    def parse(self, raw):
        if raw["mode"] == "fanmatch":
            return raw["data"].get("games", [])

        reader = csv.DictReader(io.StringIO(raw["data"]))
        rows = []
        for row in reader:
            def f(key):
                v = row.get(key, "")
                try: return float(v) if v else None
                except: return None

            def i(key):
                v = row.get(key, "")
                try: return int(v) if v else None
                except: return None

            rows.append({
                "name":        row.get("Team", ""),
                "kenpom_rank": i("KenPom_Rank"),
                "adj_o":       f("AdjO"),
                "adj_d":       f("AdjD"),
                "adj_t":       f("AdjT"),
                "three_p_pct": f("3P_Pct"),
                "three_par":   f("3PAr"),
                "ftr":         f("FTR"),
                "to_pct":      f("TO_Pct"),
                "orb_pct":     f("ORB_Pct"),
                "block_pct":   f("Block_Pct"),
                "steal_pct":   f("Steal_Pct"),
                "opp_3p_pct":  f("Opp_3P_Pct"),
                "experience":  f("Experience"),
            })
        return rows

    def validate(self, records):
        if not records:
            return []
        # FanMatch dicts pass through — no Pydantic model yet
        if "kp_winner" in records[0]:
            return records
        return [CBBTeam(**r) for r in records]

    def upsert(self, records):
        storage  = StorageManager(self.config["bucket"])
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str   = datetime.now(timezone.utc).strftime("%H%M%S")

        raw_path = getattr(self, "_raw_path", None)
        raw_mode = getattr(self, "_raw_mode", "ratings")

        if raw_path and os.path.exists(raw_path):
            ext     = "json" if raw_mode == "fanmatch" else "csv"
            gcs_raw = f"raw/kenpom/{date_str}/{raw_mode}_{ts_str}.{ext}"
            storage.write_raw_file(gcs_raw, raw_path)
            print(f"  [KenPom] Raw file → gs://{self.config['bucket']}/{gcs_raw}")

        if records and isinstance(records[0], CBBTeam):
            payload = {
                "updated":    datetime.now(timezone.utc).isoformat(),
                "team_count": len(records),
                "teams":      [r.model_dump(mode="json") for r in records]
            }
        else:
            payload = {
                "updated":    datetime.now(timezone.utc).isoformat(),
                "game_count": len(records),
                "games":      records
            }

        url = storage.write_json(self.config["gcs_object"], payload)
        print(f"  [KenPom] Parsed output → {url}")
