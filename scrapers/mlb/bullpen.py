"""
scrapers/mlb/bullpen.py

Scrapes team bullpen metrics from Fangraphs.
Filters to relief-only pitching using the Fangraphs "role" filter (RP/reliever).

Separation from starter metrics is critical for betting models — bullpen
strength drives late-game run prevention independently of the starter.

Output: mlb/bullpen.json
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

logger = logging.getLogger(__name__)

FANGRAPHS_API = "https://www.fangraphs.com/api/leaders/major-league/data"

BULLPEN_STATS = (
    "G,GS,IP,TBF,ERA,FIP,xFIP,SIERA,WHIP,"
    "K%,BB%,K-BB%,K/9,BB/9,"
    "GB%,FB%,LD%,"
    "Hard%,Med%,Soft%,Barrel%,"
    "HR/9,HR/FB,"
    "AVG,BABIP,LOB%,"
    "SwStr%,Zone%,O-Swing%,Z-Swing%"
)

SOURCE = "fangraphs"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class BullpenRecord(BaseModel):
    team: str
    season: int
    innings_pitched: Optional[float]
    games: Optional[int]
    era: Optional[float]
    fip: Optional[float]
    xfip: Optional[float]
    siera: Optional[float]
    whip: Optional[float]
    k_pct: Optional[float]
    bb_pct: Optional[float]
    k_minus_bb_pct: Optional[float]
    gb_pct: Optional[float]
    hard_hit_pct: Optional[float]
    barrel_pct: Optional[float]
    hr_per_9: Optional[float]
    lob_pct: Optional[float]
    swstr_pct: Optional[float]
    source: str
    fetched_at: str


class BullpenSnapshot(BaseModel):
    updated: str
    team_count: int
    season: int
    pitching_role: str
    bullpens: list[BullpenRecord]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(val) -> Optional[float]:
    if val is None or val == "" or val in ("- -", "--"):
        return None
    try:
        return float(str(val).replace("%", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _int(val) -> Optional[int]:
    if val is None or val == "" or val in ("- -", "--"):
        return None
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def _build_record(row: dict, season: int, fetched_at: str) -> dict:
    team_raw = row.get("Team") or row.get("team") or ""
    team = to_canonical(team_raw) if team_raw else team_raw

    return {
        "team": team,
        "season": season,
        "innings_pitched": _float(row.get("IP")),
        "games": _int(row.get("G")),
        "era": _float(row.get("ERA")),
        "fip": _float(row.get("FIP")),
        "xfip": _float(row.get("xFIP")),
        "siera": _float(row.get("SIERA")),
        "whip": _float(row.get("WHIP")),
        "k_pct": _float(row.get("K%")),
        "bb_pct": _float(row.get("BB%")),
        "k_minus_bb_pct": _float(row.get("K-BB%")),
        "gb_pct": _float(row.get("GB%")),
        "hard_hit_pct": _float(row.get("Hard%")),
        "barrel_pct": _float(row.get("Barrel%")),
        "hr_per_9": _float(row.get("HR/9")),
        "lob_pct": _float(row.get("LOB%")),
        "swstr_pct": _float(row.get("SwStr%")),
        "source": SOURCE,
        "fetched_at": fetched_at,
    }


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class BullpenScraper(BaseScraper):
    """
    Fetches team-level bullpen (relief pitching) leaderboard from Fangraphs.

    Uses Fangraphs "starter" = 0 (reliever) filter to isolate RP innings.
    The 'team=0,ts' parameter aggregates to team level.

    Minimum IP filter (default 20 RP innings) applied to ensure data quality
    at the start of the season. Adjust via config['min_ip'].
    """

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def fetch(self) -> dict:
        season = self._get_season()
        min_ip = int(self.config.get("min_ip", 20))

        params = {
            "pos": "all",
            "stats": "rel",   # "rel" = relievers only in Fangraphs API
            "lg": "all",
            "qual": str(min_ip),
            "season": season,
            "season1": season,
            "ind": 0,
            "team": "0,ts",
            "rost": 0,
            "age": 0,
            "filter": "",
            "players": 0,
            "startdate": f"{season}-01-01",
            "enddate": f"{season}-12-31",
            "columns": BULLPEN_STATS,
            "pageitems": 50,
            "pagenum": 1,
        }

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; sports-data-scraper/1.0; "
                "+https://github.com/jfmoon/sports-data-scraper)"
            ),
            "Referer": "https://www.fangraphs.com/leaders.aspx",
        }

        logger.info("Fetching Fangraphs bullpen leaderboard season=%d", season)
        resp = requests.get(FANGRAPHS_API, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data:
            raise RuntimeError(
                f"Fangraphs bullpen API missing 'data' key. Got: {list(data.keys())}"
            )

        return data

    def content_key(self, raw: dict) -> str:
        rows = raw.get("data", [])
        return "|".join(
            sorted(f"{r.get('Team')}:{r.get('ERA')}:{r.get('xFIP')}" for r in rows)
        )

    def parse(self, raw: dict) -> list[dict]:
        season = self._get_season()
        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = raw.get("data", [])

        if not rows:
            logger.warning("Fangraphs bullpen: no rows returned — possible off-season or API change")
            return []

        first = rows[0]
        required_cols = {"ERA", "IP", "K%", "BB%", "xFIP"}
        missing = required_cols - set(first.keys())
        if missing:
            raise RuntimeError(
                f"Fangraphs bullpen response missing columns: {missing}. "
                f"Available: {set(first.keys())}"
            )

        records = []
        for row in rows:
            # Skip league-average or "- -" rows that Fangraphs sometimes injects.
            team = row.get("Team") or row.get("team") or ""
            if not team or team.lower() in ("avg", "total", "- -", "league"):
                continue
            try:
                records.append(_build_record(row, season, fetched_at))
            except Exception as e:
                logger.warning("Bullpen row skipped: %s | team=%s", e, team)

        logger.info("Parsed %d team bullpen records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[BullpenRecord]:
        validated = []
        for r in records:
            try:
                validated.append(BullpenRecord(**r))
            except Exception as e:
                logger.warning("Invalid bullpen record: %s | team=%s", e, r.get("team"))
        return validated

    def upsert(self, validated: list[BullpenRecord]) -> None:
        season = self._get_season()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()

        payload = BullpenSnapshot(
            updated=fetched_at,
            team_count=len(validated),
            season=season,
            pitching_role="reliever",
            bullpens=validated,
        ).model_dump(mode="json")

        sm.persist_raw(source="mlb_bullpen", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/bullpen.json (%d teams)", len(validated))
