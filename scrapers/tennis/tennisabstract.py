"""
Tennis Abstract scraper wrapper for the sports-data-scraper framework.

Delegates all scraping to tennisabstract_scraper.py (Playwright + curl_cffi).

After the scrape completes it:
  1. Uploads raw JSON  → GCS: raw/tennisabstract/{date}/players_{ts}.json
  2. Writes parsed     → GCS: tennis/players.json

Config keys (in config.yaml under tennisabstract):
  top: 250
  slug: null              # single player slug for testing
  priority_players:       # always scraped — failure = hard error
    - {name: "Emma Raducanu", slug: "EmmaRaducanu"}
  optional_players:       # always attempted — failure = warning only
    - {name: "Danielle Collins", slug: "DanielleCollins"}
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field
from base.scraper import BaseScraper
from base.storage import StorageManager


TENNISABSTRACT_SCRAPER = os.path.join(
    os.path.dirname(__file__), "tennisabstract_scraper.py"
)


# ── Pydantic models matching actual tennisabstract_scraper.py output ──────────

class EloRatings(BaseModel):
    elo:      Optional[int] = None
    eloRank:  Optional[int] = None
    hElo:     Optional[int] = None
    hEloRank: Optional[int] = None
    cElo:     Optional[int] = None
    cEloRank: Optional[int] = None
    gElo:     Optional[int] = None
    gEloRank: Optional[int] = None


class PlayerRatings(BaseModel):
    forehand:    int = Field(default=5, ge=1, le=10)
    backhand:    int = Field(default=5, ge=1, le=10)
    serve:       int = Field(default=5, ge=1, le=10)
    netPlay:     int = Field(default=5, ge=1, le=10)
    movement:    int = Field(default=5, ge=1, le=10)
    spinHeavy:   int = Field(default=5, ge=1, le=10)
    consistency: int = Field(default=5, ge=1, le=10)
    aggression:  int = Field(default=5, ge=1, le=10)
    mentalGame:  int = Field(default=5, ge=1, le=10)
    returnGame:  int = Field(default=5, ge=1, le=10)
    variety:     int = Field(default=5, ge=1, le=10)
    riskTaking:  int = Field(default=5, ge=1, le=10)


class RecentMatch(BaseModel):
    date:       str = ""
    tournament: str = ""
    surface:    str = ""
    round:      str = ""
    opponent:   str = ""
    score:      str = ""
    result:     str = ""


class DataAvailability(BaseModel):
    hasChartingServe:   bool = False
    hasChartingReturn:  bool = False
    hasChartingRally:   bool = False
    hasChartingTactics: bool = False


class WtaPlayer(BaseModel):
    name:             str
    slug:             str
    country:          str = ""
    emoji:            str = ""
    rank:             Optional[int] = None
    lastUpdated:      str = ""
    ratings:          PlayerRatings = Field(default_factory=PlayerRatings)
    elo:              EloRatings = Field(default_factory=EloRatings)
    recentMatches:    List[Dict[str, Any]] = Field(default_factory=list)
    dataAvailability: DataAvailability = Field(default_factory=DataAvailability)


# ── Scraper ───────────────────────────────────────────────────────────────────

class TennisAbstractScraper(BaseScraper):

    def _call_scraper(self, slug: Optional[str], top_n: int, out_path: str) -> bool:
        """
        Run tennisabstract_scraper.py as a subprocess.
        Returns True if output file is non-empty, False on any failure.
        Timeout: 120s per player, 3600s for full ranked run.
        """
        env = os.environ.copy()
        env.update({
            "OUTPUT_PATH": out_path,
            "TOP_N":       str(top_n),
            "GCS_BUCKET":  self.config.get("bucket", ""),
        })
        cmd     = [sys.executable, TENNISABSTRACT_SCRAPER]
        timeout = 120 if slug else 3600
        if slug:
            cmd.append(slug)

        try:
            subprocess.run(cmd, check=True, env=env, timeout=timeout)
            return os.path.exists(out_path) and os.path.getsize(out_path) > 0
        except subprocess.TimeoutExpired:
            print(f"  [Timeout] {slug or f'top-{top_n}'} exceeded {timeout}s")
            return False
        except subprocess.CalledProcessError as e:
            print(f"  [Script Error] {slug or f'top-{top_n}'} exited {e.returncode}")
            return False

    def fetch(self):
        top              = self.config.get("top", 250)
        slug             = self.config.get("slug", None)
        priority_players = self.config.get("priority_players", [])
        optional_players = self.config.get("optional_players", [])
        date_str         = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        ts_str           = datetime.now(timezone.utc).strftime("%H%M%S")
        out_dir          = f"data/raw/tennisabstract/{date_str}"
        os.makedirs(out_dir, exist_ok=True)

        all_players = []
        hard_errors = []
        soft_warns  = []

        # ── Single-player test mode ───────────────────────────────────────────
        if slug:
            out_path = f"{out_dir}/players_{ts_str}.json"
            print(f"\n[TennisAbstract] Single player: {slug}")
            if not self._call_scraper(slug, 0, out_path):
                raise RuntimeError(f"Failed to scrape: {slug}")
            with open(out_path) as f:
                data = json.load(f)
            self._raw_path = out_path
            return {"path": out_path, "data": data}

        # ── Step 1: top N by ranking ──────────────────────────────────────────
        ranked_path = f"{out_dir}/ranked_{ts_str}.json"
        print(f"\n[TennisAbstract] Scraping top {top} WTA players...")
        if self._call_scraper(None, top, ranked_path):
            with open(ranked_path) as f:
                ranked_data = json.load(f)
            all_players = ranked_data.get("players", [])
            hard_errors = ranked_data.get("errors", [])
        else:
            print("  ❌ Critical: ranked list fetch failed")

        scraped_slugs = {p["slug"] for p in all_players}

        # ── Step 2 & 3: priority then optional extras ─────────────────────────
        for group, is_optional in [("priority_players", False),
                                   ("optional_players",  True)]:
            extras = [p for p in self.config.get(group, [])
                      if p["slug"] not in scraped_slugs]
            if not extras:
                continue
            label = "optional" if is_optional else "priority"
            print(f"\n[TennisAbstract] {len(extras)} {label} players outside top {top}...")

            for player in extras:
                out_path = f"{out_dir}/{label}_{player['slug']}_{ts_str}.json"
                ok = self._call_scraper(player["slug"], 0, out_path)
                if ok:
                    with open(out_path) as f:
                        p_data = json.load(f)
                    players = p_data.get("players", [])
                    if players:
                        all_players.extend(players)
                        scraped_slugs.add(player["slug"])
                        print(f"  ✅ {player['name']}")
                        continue

                # Failed
                if is_optional:
                    soft_warns.append(player["name"])
                    print(f"  ⚠️  {player['name']} — sparse/inactive, skipped")
                else:
                    hard_errors.append(player["name"])
                    print(f"  ❌ {player['name']} — hard error")

        if soft_warns:
            print(f"\n[TennisAbstract] Skipped: {', '.join(soft_warns)}")
        if hard_errors:
            print(f"[TennisAbstract] ❌ Failed: {', '.join(hard_errors)}")

        # ── Write merged file ─────────────────────────────────────────────────
        merged_path = f"{out_dir}/players_{ts_str}.json"
        merged = {
            "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "playerCount": len(all_players),
            "players":     all_players,
            "errors":      hard_errors,
            "warnings":    soft_warns,
        }
        with open(merged_path, "w") as f:
            json.dump(merged, f, indent=2)

        self._raw_path = merged_path
        return {"path": merged_path, "data": merged}

    def content_key(self, raw):
        d = raw.get("data", {})
        return {
            "lastUpdated": d.get("lastUpdated"),
            "playerCount": d.get("playerCount"),
        }

    def parse(self, raw):
        return raw["data"].get("players", [])

    def validate(self, records):
        valid = []
        for r in records:
            try:
                valid.append(WtaPlayer(**r).model_dump())
            except Exception as e:
                print(f"  [Validation Skip] {r.get('name', 'unknown')}: {e}")
        return valid

    def upsert(self, records):
        storage = StorageManager(self.config["bucket"])

        raw_path = getattr(self, "_raw_path", None)
        if raw_path and os.path.exists(raw_path):
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            ts_str   = datetime.now(timezone.utc).strftime("%H%M%S")
            gcs_raw  = f"raw/tennisabstract/{date_str}/players_{ts_str}.json"
            storage.write_raw_file(gcs_raw, raw_path)
            print(f"  [TennisAbstract] Raw → gs://{self.config['bucket']}/{gcs_raw}")

        payload = {
            "updated":      datetime.now(timezone.utc).isoformat(),
            "player_count": len(records),
            "players":      records,
        }
        url = storage.write_json(self.config["gcs_object"], payload)
        print(f"  [TennisAbstract] Output → {url}")
