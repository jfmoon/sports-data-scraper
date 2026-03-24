"""
scrapers/mlb/fangraphs_teams.py

Scrapes team batting metrics from Fangraphs, including splits vs LHP/RHP.
Uses the Fangraphs leaderboard JSON API with split filtering.

Produces three split contexts per team:
  - overall
  - vs_lhp
  - vs_rhp

Output: mlb/teams.json
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

BATTING_STATS = (
    "PA,AB,H,1B,2B,3B,HR,R,RBI,BB,IBB,SO,HBP,SF,SH,GDP,SB,CS,"
    "AVG,OBP,SLG,OPS,ISO,BABIP,wOBA,wRC+,WAR,"
    "BB%,K%,BB/K,"
    "LD%,GB%,FB%,IFFB%,HR/FB,"
    "Pull%,Cent%,Oppo%,"
    "Hard%,Med%,Soft%,Barrel%,"
    "O-Swing%,Z-Swing%,Swing%,O-Contact%,Z-Contact%,Contact%,SwStr%,Zone%"
)

# Fangraphs split parameter values for vs-handedness splits
SPLITS = {
    "overall": None,  # no split filter
    "vs_lhp": "vl",   # vs left-handed pitcher
    "vs_rhp": "vr",   # vs right-handed pitcher
}

SOURCE = "fangraphs"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TeamBattingRecord(BaseModel):
    team: str
    season: int
    split: str
    pa: Optional[int]
    avg: Optional[float]
    obp: Optional[float]
    slg: Optional[float]
    ops: Optional[float]
    iso: Optional[float]
    woba: Optional[float]
    wrc_plus: Optional[float]
    k_pct: Optional[float]
    bb_pct: Optional[float]
    barrel_pct: Optional[float]
    hard_hit_pct: Optional[float]
    gb_pct: Optional[float]
    fb_pct: Optional[float]
    swstr_pct: Optional[float]
    source: str
    fetched_at: str


class TeamBattingSnapshot(BaseModel):
    updated: str
    team_count: int
    season: int
    splits_available: list[str]
    teams: list[TeamBattingRecord]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(val) -> Optional[float]:
    if val is None or val == "" or val in ("- -", "--", "null"):
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


def _build_record(row: dict, split: str, season: int, fetched_at: str) -> dict:
    team_raw = row.get("Team") or row.get("team") or ""
    team = to_canonical(team_raw) if team_raw else team_raw

    return {
        "team": team,
        "season": season,
        "split": split,
        "pa": _int(row.get("PA")),
        "avg": _float(row.get("AVG")),
        "obp": _float(row.get("OBP")),
        "slg": _float(row.get("SLG")),
        "ops": _float(row.get("OPS")),
        "iso": _float(row.get("ISO")),
        "woba": _float(row.get("wOBA")),
        "wrc_plus": _float(row.get("wRC+")),
        "k_pct": _float(row.get("K%")),
        "bb_pct": _float(row.get("BB%")),
        "barrel_pct": _float(row.get("Barrel%")),
        "hard_hit_pct": _float(row.get("Hard%")),
        "gb_pct": _float(row.get("GB%")),
        "fb_pct": _float(row.get("FB%")),
        "swstr_pct": _float(row.get("SwStr%")),
        "source": SOURCE,
        "fetched_at": fetched_at,
    }


def _fetch_split(season: int, split_key: str, split_val: Optional[str]) -> list[dict]:
    """Fetch team batting rows for a single split context."""
    params = {
        "pos": "all",
        "stats": "bat",
        "lg": "all",
        "qual": "0",
        "season": season,
        "season1": season,
        "ind": 0,
        "team": "0,ts",  # team stats aggregation
        "rost": 0,
        "age": 0,
        "filter": "",
        "players": 0,
        "startdate": f"{season}-01-01",
        "enddate": f"{season}-12-31",
        "columns": BATTING_STATS,
        "pageitems": 50,
        "pagenum": 1,
    }
    if split_val:
        params["split"] = split_val

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; sports-data-scraper/1.0; "
            "+https://github.com/jfmoon/sports-data-scraper)"
        ),
        "Referer": "https://www.fangraphs.com/leaders.aspx",
    }

    logger.info("Fetching Fangraphs team batting split=%s season=%d", split_key, season)
    resp = requests.get(FANGRAPHS_API, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "data" not in data:
        raise RuntimeError(
            f"Fangraphs team batting API missing 'data' key for split={split_key}. "
            f"Got: {list(data.keys())}"
        )

    return data["data"]


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class FangraphsTeamsScraper(BaseScraper):
    """
    Fetches team batting leaderboard from Fangraphs for overall, vs_lhp, vs_rhp splits.
    Makes three API calls (one per split). Validates expected columns on first row of each.
    """

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def fetch(self) -> dict:
        season = self._get_season()
        all_data = {}

        fetch_errors = []
        for split_key, split_val in SPLITS.items():
            try:
                rows = _fetch_split(season, split_key, split_val)
                all_data[split_key] = rows
            except Exception as e:
                # A single split failure (e.g. Fangraphs 500 during daily refresh)
                # must not discard the splits that succeeded. Log loudly and store
                # an empty list so parse() can warn and skip this split cleanly.
                logger.error(
                    "Fangraphs team batting fetch failed for split=%s: %s — "
                    "this split will be absent from output",
                    split_key, e
                )
                all_data[split_key] = []
                fetch_errors.append(split_key)

        if fetch_errors and len(fetch_errors) == len(SPLITS):
            # Every split raised an exception — genuine API failure, not pre-season empty.
            raise RuntimeError(
                "Fangraphs team batting: all splits raised errors. No data to write."
            )

        if not any(all_data.values()):
            # All splits returned empty rows — pre-season or no data yet. Warn and write empty.
            logger.warning(
                "Fangraphs team batting: all splits returned 0 rows — "
                "pre-season or no data available yet. Writing empty snapshot."
            )

        return {"season": season, "splits": all_data}

    def content_key(self, raw: dict) -> str:
        """Hash on team names + wRC+ across all splits."""
        parts = []
        for split_key, rows in raw.get("splits", {}).items():
            for r in rows:
                parts.append(f"{split_key}:{r.get('Team')}:{r.get('wRC+')}")
        return "|".join(sorted(parts))

    def parse(self, raw: dict) -> list[dict]:
        season = raw["season"]
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = []

        required_cols = {"AVG", "OBP", "SLG", "wOBA", "wRC+", "K%", "BB%"}

        for split_key, rows in raw["splits"].items():
            if not rows:
                logger.warning("Fangraphs teams: no rows for split=%s — skipping", split_key)
                continue

            first = rows[0]
            missing = required_cols - set(first.keys())
            if missing:
                raise RuntimeError(
                    f"Fangraphs team batting missing expected columns for split={split_key}: "
                    f"{missing}. Available: {set(first.keys())}"
                )

            for row in rows:
                try:
                    records.append(_build_record(row, split_key, season, fetched_at))
                except Exception as e:
                    logger.warning(
                        "Fangraphs team row skipped: %s | split=%s team=%s",
                        e, split_key, row.get("Team")
                    )

        logger.info("Parsed %d Fangraphs team batting records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[TeamBattingRecord]:
        validated = []
        for r in records:
            try:
                validated.append(TeamBattingRecord(**r))
            except Exception as e:
                logger.warning("Invalid team batting record: %s | team=%s split=%s", e, r.get("team"), r.get("split"))
        return validated

    def upsert(self, validated: list[TeamBattingRecord]) -> None:
        season = self._get_season()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()

        # Deduplicate team count from overall split only.
        overall_teams = {r.team for r in validated if r.split == "overall"}

        payload = TeamBattingSnapshot(
            updated=fetched_at,
            team_count=len(overall_teams),
            season=season,
            splits_available=list(SPLITS.keys()),
            teams=validated,
        ).model_dump(mode="json")

        sm.persist_raw(source="mlb_teams", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info(
            "Wrote mlb/teams.json (%d teams × %d splits = %d records)",
            len(overall_teams), len(SPLITS), len(validated)
        )
