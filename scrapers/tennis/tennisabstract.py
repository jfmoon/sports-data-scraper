"""
Tennis Abstract scraper wrapper for the sports-data-scraper framework.

Delegates all scraping to tennisabstract_scraper.py (Playwright + curl_cffi).
Scrapes WTA player profiles, career stats, Elo ratings, recent match results,
and computes 1-10 attribute scores for the WTA Style Classifier.

After the scrape completes it:
  1. Uploads raw JSON  → GCS: raw/tennisabstract/{date}/players_{ts}.json
  2. Writes parsed     → GCS: tennis/players.json

Config keys (in config.yaml under tennisabstract):
  top: 250                # number of top-ranked players to scrape
  slug: null              # single player slug for testing (e.g. IgaSwiatek)
  priority_players:       # always scraped, failure = hard error
    - {name: "Emma Raducanu", slug: "EmmaRaducanu"}
  optional_players:       # always attempted, failure = warning only
    - {name: "Danielle Collins", slug: "DanielleCollins"}
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

    def _run_single(self, slug, out_path, env):
        """Run scraper for one player slug. Returns parsed data dict or None."""
        env = {**env, "OUTPUT_PATH": out_path, "TOP_N": "0"}
        try:
            subprocess.run(
                [sys.executable, TENNISABSTRACT_SCRAPER, slug],
                check=True, env=env
            )
            with open(out_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"  ⚠️  Failed {slug}: {e}")
            return None

    def fetch(self):
        top              = self.config.get("top", 250)
        slug             = self.config.get("slug", None)
        priority_players = self.config.get("priority_players", [])
        optional_players = self.config.get("optional_players", [])
        date_str         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str           = datetime.now(timezone.utc).strftime("%H%M%S")
        out_dir          = f"data/raw/tennisabstract/{date_str}"
        os.makedirs(out_dir, exist_ok=True)

        base_env = os.environ.copy()
        base_env["GCS_BUCKET"] = self.config.get("bucket", "")

        # ── Single-player test mode ───────────────────────────────────────────
        if slug:
            out_path = f"{out_dir}/players_{ts_str}.json"
            print(f"\n[TennisAbstract] Single player mode: {slug}")
            data = self._run_single(slug, out_path, base_env)
            if not data:
                raise RuntimeError(f"Failed to scrape single player: {slug}")
            self._raw_path = out_path
            self._raw_mode = "players"
            return {"mode": "players", "path": out_path, "data": data}

        # ── Step 1: top N by ranking ──────────────────────────────────────────
        ranked_path = f"{out_dir}/ranked_{ts_str}.json"
        env = {**base_env, "OUTPUT_PATH": ranked_path, "TOP_N": str(top)}
        print(f"\n[TennisAbstract] Scraping top {top} WTA players...")
        subprocess.run([sys.executable, TENNISABSTRACT_SCRAPER],
                       check=True, env=env)

        with open(ranked_path, "r") as f:
            ranked_data = json.load(f)

        all_results  = ranked_data.get("players", [])
        hard_errors  = ranked_data.get("errors", [])
        soft_warns   = []
        ranked_slugs = {p["slug"] for p in all_results}

        # ── Step 2: priority players not in top N (hard failures) ─────────────
        extras = [p for p in priority_players if p["slug"] not in ranked_slugs]
        if extras:
            print(f"\n[TennisAbstract] {len(extras)} priority players outside top {top}...")
            for player in extras:
                out_path = f"{out_dir}/priority_{player['slug']}_{ts_str}.json"
                data = self._run_single(player["slug"], out_path, base_env)
                if data and data.get("players"):
                    all_results.extend(data["players"])
                    ranked_slugs.add(player["slug"])
                    print(f"  ✅ {player['name']}")
                else:
                    hard_errors.append(player["name"])
                    print(f"  ❌ {player['name']} (hard error)")

        # ── Step 3: optional players not yet scraped (soft failures) ──────────
        optionals = [p for p in optional_players if p["slug"] not in ranked_slugs]
        if optionals:
            print(f"\n[TennisAbstract] {len(optionals)} optional players outside top {top}...")
            for player in optionals:
                out_path = f"{out_dir}/optional_{player['slug']}_{ts_str}.json"
                data = self._run_single(player["slug"], out_path, base_env)
                if data and data.get("players"):
                    all_results.extend(data["players"])
                    print(f"  ✅ {player['name']}")
                else:
                    soft_warns.append(player["name"])
                    print(f"  ⚠️  {player['name']} (sparse/inactive page — skipped)")

        # ── Write merged output ───────────────────────────────────────────────
        merged_path = f"{out_dir}/players_{ts_str}.json"
        merged = {
            "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "playerCount": len(all_results),
            "players":     all_results,
            "errors":      hard_errors,
            "warnings":    soft_warns,
        }
        with open(merged_path, "w") as f:
            json.dump(merged, f, indent=2)

        if soft_warns:
            print(f"\n[TennisAbstract] ⚠️  Skipped (inactive/sparse): {', '.join(soft_warns)}")
        if hard_errors:
            print(f"[TennisAbstract] ❌ Failed: {', '.join(hard_errors)}")

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
