"""
scrapers/mlb/statcast_hitters.py

Scrapes Baseball Savant expected-stat leaderboard for hitters.
Part of the statcast split: statcast_pitchers.py + statcast_hitters.py replace
the original statcast.py, which violated the BaseScraper list[dict] contract.

Output: mlb/statcast_hitters.json
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

# Re-use fetch/parse helpers from statcast_pitchers to avoid duplication.
from scrapers.mlb.statcast_pitchers import (
    _fetch_csv,
    _verify_columns,
    _float,
    _int,
    _normalize_name,
)

logger = logging.getLogger(__name__)

SOURCE = "baseball_savant"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class StatcastHitterRecord(BaseModel):
    player_id: Optional[str]
    name: str
    team: str
    season: int
    pa: Optional[int]
    xba: Optional[float]
    xslg: Optional[float]
    xwoba: Optional[float]
    barrel_pct: Optional[float]
    hard_hit_pct: Optional[float]
    avg_exit_velocity: Optional[float]
    whiff_pct: Optional[float]
    k_pct: Optional[float]
    bb_pct: Optional[float]
    source: str
    fetched_at: str


class StatcastHittersSnapshot(BaseModel):
    # Standard envelope — schema_version 1
    schema_version: int = 1
    generated_at: str
    scraper_key: str = "mlb_statcast_hitters"
    record_count: int
    warnings: list[str] = []
    # Existing fields — unchanged
    updated: str
    season: int
    hitter_count: int
    hitters: list[StatcastHitterRecord]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_row(row: dict, season: int, fetched_at: str) -> dict:
    team_raw = row.get("team_name_abb") or row.get("team") or ""
    team = to_canonical(team_raw) if team_raw else team_raw
    name = _normalize_name(
        row.get("last_name, first_name") or row.get("player_name") or row.get("name") or ""
    )
    return {
        "player_id": row.get("player_id") or None,
        "name": name, "team": team, "season": season,
        "pa": _int(row.get("pa")),
        "xba": _float(row.get("est_ba") or row.get("xba")),
        "xslg": _float(row.get("est_slg") or row.get("xslg")),
        "xwoba": _float(row.get("est_woba") or row.get("xwoba")),
        "barrel_pct": _float(row.get("barrel_batted_rate") or row.get("barrel_pct")),
        "hard_hit_pct": _float(row.get("hard_hit_percent")),
        "avg_exit_velocity": _float(row.get("exit_velocity_avg")),
        "whiff_pct": _float(row.get("whiff_percent")),
        "k_pct": _float(row.get("k_percent")),
        "bb_pct": _float(row.get("bb_percent")),
        "source": SOURCE, "fetched_at": fetched_at,
    }


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class StatcastHittersScraper(BaseScraper):

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def fetch(self) -> dict:
        season = self._get_season()
        min_pa = int(self.config.get("min_pa", 20))
        rows = _fetch_csv(season, "batter", min_pa)
        return {"season": season, "rows": rows}

    def content_key(self, raw: dict) -> str:
        return "|".join(sorted(
            f"h:{r.get('player_id')}:{r.get('est_woba') or r.get('xwoba')}"
            for r in raw.get("rows", [])
        ))

    def parse(self, raw: dict) -> list[dict]:
        season = raw["season"]
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = raw.get("rows", [])
        if not rows:
            logger.warning("Statcast hitters: no rows returned")
            return []
        _verify_columns(rows, required={"pa", "player_id"}, context="hitter")
        records = []
        for row in rows:
            try:
                records.append(_parse_row(row, season, fetched_at))
            except Exception as e:
                logger.warning("Statcast hitter row skipped: %s | id=%s", e, row.get("player_id"))
        logger.info("Parsed %d statcast hitter records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[StatcastHitterRecord]:
        validated = []
        for r in records:
            try:
                validated.append(StatcastHitterRecord(**r))
            except Exception as e:
                logger.warning("Invalid statcast hitter: %s | name=%s", e, r.get("name"))
        return validated

    def upsert(self, validated: list[StatcastHitterRecord]) -> None:
        season = self._get_season()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()
        payload = StatcastHittersSnapshot(
            generated_at=fetched_at,
            record_count=len(validated),
            updated=fetched_at,
            season=season,
            hitter_count=len(validated),
            hitters=validated,
        ).model_dump(mode="json")
        sm.persist_raw(source="mlb_statcast_hitters", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/statcast_hitters.json (%d hitters)", len(validated))
