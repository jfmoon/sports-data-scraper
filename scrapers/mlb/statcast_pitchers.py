"""
scrapers/mlb/statcast_pitchers.py

Scrapes Baseball Savant expected-stat leaderboard for pitchers.
Part of the statcast split: statcast_pitchers.py + statcast_hitters.py replace
the original statcast.py, which violated the BaseScraper list[dict] contract.

Output: mlb/statcast_pitchers.json
"""

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

logger = logging.getLogger(__name__)

SAVANT_URL = "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
SOURCE = "baseball_savant"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class StatcastPitcherRecord(BaseModel):
    player_id: Optional[str]
    name: str
    team: str
    season: int
    pa: Optional[int]
    xera: Optional[float]
    xba: Optional[float]
    xslg: Optional[float]
    xwoba: Optional[float]
    whiff_pct: Optional[float]
    k_pct: Optional[float]
    bb_pct: Optional[float]
    barrel_pct: Optional[float]
    hard_hit_pct: Optional[float]
    avg_exit_velocity: Optional[float]
    source: str
    fetched_at: str


class StatcastPitchersSnapshot(BaseModel):
    updated: str
    season: int
    pitcher_count: int
    pitchers: list[StatcastPitcherRecord]


# ---------------------------------------------------------------------------
# Helpers (shared with statcast_hitters.py — kept local to avoid circular import)
# ---------------------------------------------------------------------------

def _float(val) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "null", "None", ".", "-."):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _int(val) -> Optional[int]:
    v = _float(val)
    return int(v) if v is not None else None


def _normalize_name(raw: str) -> str:
    """Baseball Savant returns 'Last, First' — normalize to 'First Last'."""
    if ", " in raw:
        parts = [p.strip() for p in raw.split(",", 1)]
        return f"{parts[1]} {parts[0]}"
    return raw


def _fetch_csv(season: int, player_type: str, min_pa: int) -> list[dict]:
    params = {
        "type": player_type,
        "year": season,
        "position": "",
        "team": "",
        "min_pa": str(min_pa),
        "csv": "true",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; sports-data-scraper/1.0; "
            "+https://github.com/jfmoon/sports-data-scraper)"
        ),
        "Referer": "https://baseballsavant.mlb.com/leaderboard/expected_statistics",
    }

    logger.info("Fetching Baseball Savant %s leaderboard season=%d", player_type, season)
    resp = requests.get(SAVANT_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    content = resp.text.strip()
    if not content or content.startswith("<"):
        raise RuntimeError(
            f"Baseball Savant returned non-CSV response for {player_type}. "
            "Possible Cloudflare block or page change."
        )

    rows = list(csv.DictReader(io.StringIO(content)))
    if not rows:
        logger.warning("Baseball Savant: no rows for type=%s", player_type)
    return rows


def _verify_columns(rows: list[dict], required: set[str], context: str) -> None:
    if not rows:
        return
    available = set(rows[0].keys())
    missing = required - available
    if missing:
        raise RuntimeError(
            f"Baseball Savant {context} missing expected columns: {missing}. "
            f"Available: {available}"
        )


def _parse_row(row: dict, season: int, fetched_at: str) -> dict:
    team_raw = row.get("team_name_abb") or row.get("team") or ""
    team = to_canonical(team_raw) if team_raw else team_raw
    name = _normalize_name(
        row.get("last_name, first_name") or row.get("player_name") or row.get("name") or ""
    )
    return {
        "player_id": row.get("player_id") or None,
        "name": name,
        "team": team,
        "season": season,
        "pa": _int(row.get("pa")),
        "xera": _float(row.get("p_era") or row.get("xera") or row.get("est_era")),
        "xba": _float(row.get("est_ba") or row.get("xba")),
        "xslg": _float(row.get("est_slg") or row.get("xslg")),
        "xwoba": _float(row.get("est_woba") or row.get("xwoba")),
        "whiff_pct": _float(row.get("whiff_percent")),
        "k_pct": _float(row.get("k_percent")),
        "bb_pct": _float(row.get("bb_percent")),
        "barrel_pct": _float(row.get("barrel_batted_rate") or row.get("barrel_pct")),
        "hard_hit_pct": _float(row.get("hard_hit_percent")),
        "avg_exit_velocity": _float(row.get("exit_velocity_avg")),
        "source": SOURCE,
        "fetched_at": fetched_at,
    }


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class StatcastPitchersScraper(BaseScraper):
    """
    Fetches Baseball Savant expected-stats leaderboard for pitchers.
    Returns list[dict] from parse() — compliant with BaseScraper/ScraperRunner contract.
    """

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def fetch(self) -> dict:
        season = self._get_season()
        min_pa = int(self.config.get("min_pa", 20))
        rows = _fetch_csv(season, "pitcher", min_pa)
        return {"season": season, "rows": rows}

    def content_key(self, raw: dict) -> str:
        return "|".join(sorted(
            f"p:{r.get('player_id')}:{r.get('est_woba') or r.get('xwoba')}"
            for r in raw.get("rows", [])
        ))

    def parse(self, raw: dict) -> list[dict]:
        season = raw["season"]
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = raw.get("rows", [])

        if not rows:
            logger.warning("Statcast pitchers: no rows returned")
            return []

        # Verify presence of structural columns that cannot alias.
        _verify_columns(rows, required={"pa", "player_id"}, context="pitcher")

        records = []
        for row in rows:
            try:
                records.append(_parse_row(row, season, fetched_at))
            except Exception as e:
                logger.warning("Statcast pitcher row skipped: %s | id=%s", e, row.get("player_id"))

        logger.info("Parsed %d statcast pitcher records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[StatcastPitcherRecord]:
        validated = []
        for r in records:
            try:
                validated.append(StatcastPitcherRecord(**r))
            except Exception as e:
                logger.warning("Invalid statcast pitcher: %s | name=%s", e, r.get("name"))
        return validated

    def upsert(self, validated: list[StatcastPitcherRecord]) -> None:
        season = self._get_season()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()

        payload = StatcastPitchersSnapshot(
            updated=fetched_at,
            season=season,
            pitcher_count=len(validated),
            pitchers=validated,
        ).model_dump(mode="json")

        sm.persist_raw(source="mlb_statcast_pitchers", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/statcast_pitchers.json (%d pitchers)", len(validated))
