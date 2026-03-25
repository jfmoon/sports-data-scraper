"""
scrapers/mlb/fangraphs_pitchers.py

Scrapes pitcher-level advanced metrics from Fangraphs leaderboards.
Uses the Fangraphs data export API (JSON) — same endpoint the leaderboard page
calls internally. No HTML parsing required.

Endpoint: https://www.fangraphs.com/api/leaders/major-league/data
Output: mlb/pitchers.json
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from pydantic import BaseModel, field_validator

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

logger = logging.getLogger(__name__)

FANGRAPHS_API = "https://www.fangraphs.com/api/leaders/major-league/data"

PITCHER_STATS = (
    "W,L,ERA,G,GS,IP,TBF,H,R,ER,HR,BB,IBB,HBP,WP,SO,"
    "K/9,BB/9,K/BB,H/9,HR/9,AVG,WHIP,BABIP,LOB%,"
    "ERA-,FIP-,xFIP-,FIP,xFIP,SIERA,"
    "K%,BB%,K-BB%,GB%,FB%,LD%,IFFB%,"
    "Hard%,Med%,Soft%,Barrel%,"
    "Pull%,Cent%,Oppo%,"
    "Zone%,O-Swing%,Z-Swing%,Swing%,O-Contact%,Z-Contact%,Contact%,SwStr%"
)

SOURCE = "fangraphs"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class PitcherRecord(BaseModel):
    pitcher_id: Optional[str]
    name: str
    team: str
    throws: Optional[str]
    season: int
    games: Optional[int]
    games_started: Optional[int]
    innings_pitched: Optional[float]
    era: Optional[float]
    xfip: Optional[float]
    siera: Optional[float]
    fip: Optional[float]
    whip: Optional[float]
    k_pct: Optional[float]
    bb_pct: Optional[float]
    k_minus_bb_pct: Optional[float]
    gb_pct: Optional[float]
    hard_hit_pct: Optional[float]
    barrel_pct: Optional[float]
    hr_per_9: Optional[float]
    swstr_pct: Optional[float]
    source: str
    fetched_at: str

    @field_validator("throws", mode="before")
    @classmethod
    def normalize_hand(cls, v):
        if v is None:
            return None
        v = str(v).strip().upper()
        return v if v in ("L", "R", "S") else None


class PitchersSnapshot(BaseModel):
    # Standard envelope — schema_version 1
    schema_version: int = 1
    generated_at: str
    scraper_key: str = "mlb_pitchers"
    record_count: int
    warnings: list[str] = []
    # Existing fields — unchanged
    updated: str
    pitcher_count: int
    season: int
    pitchers: list[PitcherRecord]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(val) -> Optional[float]:
    if val is None or val == "" or val == "- -" or val == "--":
        return None
    try:
        return float(str(val).replace("%", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _int(val) -> Optional[int]:
    if val is None or val == "" or val == "- -":
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def _build_record(row: dict, season: int, fetched_at: str) -> dict:
    name = row.get("PlayerName") or row.get("Name") or row.get("name") or ""
    team_raw = row.get("Team") or row.get("team") or ""
    team = to_canonical(team_raw) if team_raw else team_raw
    return {
        "pitcher_id": str(row["playerid"]) if row.get("playerid") else None,
        "name": name,
        "team": team,
        "throws": row.get("PitchingHand") or row.get("Throws") or row.get("throws"),
        "season": season,
        "games": _int(row.get("G")),
        "games_started": _int(row.get("GS")),
        "innings_pitched": _float(row.get("IP")),
        "era": _float(row.get("ERA")),
        "xfip": _float(row.get("xFIP")),
        "siera": _float(row.get("SIERA")),
        "fip": _float(row.get("FIP")),
        "whip": _float(row.get("WHIP")),
        "k_pct": _float(row.get("K%")),
        "bb_pct": _float(row.get("BB%")),
        "k_minus_bb_pct": _float(row.get("K-BB%")),
        "gb_pct": _float(row.get("GB%")),
        "hard_hit_pct": _float(row.get("Hard%")),
        "barrel_pct": _float(row.get("Barrel%")),
        "hr_per_9": _float(row.get("HR/9")),
        "swstr_pct": _float(row.get("SwStr%")),
        "source": SOURCE,
        "fetched_at": fetched_at,
    }


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class FangraphsPitchersScraper(BaseScraper):

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def fetch(self) -> dict:
        season = self._get_season()
        min_ip = int(self.config.get("min_ip", 10))
        params = {
            "pos": "all", "stats": "pit", "lg": "all",
            "qual": str(min_ip), "season": season, "season1": season,
            "ind": 0, "team": 0, "rost": 0, "age": 0, "filter": "",
            "players": 0,
            "startdate": f"{season}-01-01", "enddate": f"{season}-12-31",
            "columns": PITCHER_STATS, "pageitems": 500, "pagenum": 1,
        }
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; sports-data-scraper/1.0; "
                "+https://github.com/jfmoon/sports-data-scraper)"
            ),
            "Referer": "https://www.fangraphs.com/leaders.aspx",
        }
        logger.info("Fetching Fangraphs pitchers leaderboard season=%d", season)
        resp = requests.get(FANGRAPHS_API, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if "data" not in data:
            raise RuntimeError(
                f"Fangraphs pitcher API response missing 'data' key. "
                f"Got: {list(data.keys())}"
            )
        return data

    def content_key(self, raw: dict) -> str:
        rows = raw.get("data", [])
        keys = [f"{r.get('playerid')}:{r.get('ERA')}" for r in rows]
        return "|".join(sorted(str(k) for k in keys))

    def parse(self, raw: dict) -> list[dict]:
        season = self._get_season()
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = raw.get("data", [])
        if not rows:
            logger.warning("Fangraphs pitchers: no rows returned — possible off-season or API change")
            return []
        first = rows[0]
        required_cols = {"ERA", "IP", "K%", "BB%"}
        missing = required_cols - set(first.keys())
        if missing:
            raise RuntimeError(
                f"Fangraphs pitcher response missing expected columns: {missing}. "
                f"Available: {set(first.keys())}"
            )
        records = []
        for row in rows:
            try:
                records.append(_build_record(row, season, fetched_at))
            except Exception as e:
                logger.warning("Fangraphs pitcher row skipped: %s | row=%s", e, row.get("PlayerName"))
        logger.info("Parsed %d Fangraphs pitcher records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[PitcherRecord]:
        validated = []
        for r in records:
            try:
                validated.append(PitcherRecord(**r))
            except Exception as e:
                logger.warning("Invalid pitcher record: %s | name=%s", e, r.get("name"))
        return validated

    def upsert(self, validated: list[PitcherRecord]) -> None:
        season = self._get_season()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()
        payload = PitchersSnapshot(
            generated_at=fetched_at,
            record_count=len(validated),
            updated=fetched_at,
            pitcher_count=len(validated),
            season=season,
            pitchers=validated,
        ).model_dump(mode="json")
        sm.persist_raw(source="mlb_pitchers", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/pitchers.json (%d pitchers)", len(validated))
