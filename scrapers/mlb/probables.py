"""
scrapers/mlb/probables.py

Scrapes MLB probable starters from the official MLB Stats API.
Endpoint: https://statsapi.mlb.com/api/v1/schedule
Output: mlb/probables.json
Schema: one record per scheduled game for today + next 2 days.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from pydantic import BaseModel, field_validator

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

logger = logging.getLogger(__name__)

# MLB Stats API — stable, official, no auth required.
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
SCHEDULE_PARAMS = {
    "sportId": 1,
    "hydrate": "probablePitcher(note),team,linescore",
    "fields": (
        "dates,date,games,gamePk,gameDate,status,teams,"
        "away,home,team,name,abbreviation,"
        "probablePitcher,id,fullName,pitchHand,code"
    ),
}
SOURCE = "mlb_stats_api"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ProbablePitcherRecord(BaseModel):
    game_id: str
    date: str
    commence_time: str
    away_team: str
    home_team: str
    away_pitcher: Optional[str]
    home_pitcher: Optional[str]
    away_hand: Optional[str]
    home_hand: Optional[str]
    away_pitcher_id: Optional[int]
    home_pitcher_id: Optional[int]
    away_confirmed: bool
    home_confirmed: bool
    source: str
    fetched_at: str

    @field_validator("away_hand", "home_hand", mode="before")
    @classmethod
    def normalize_hand(cls, v):
        if v is None:
            return None
        v = str(v).strip().upper()
        if v in ("L", "R", "S"):
            return v
        return None


class ProbablesSnapshot(BaseModel):
    # Standard envelope — schema_version 1
    schema_version: int = 1
    generated_at: str
    scraper_key: str = "mlb_probables"
    record_count: int
    warnings: list[str] = []
    # Existing fields — unchanged
    updated: str
    game_count: int
    games: list[ProbablePitcherRecord]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_pitcher(pitcher_dict: Optional[dict]) -> tuple[
    Optional[str], Optional[str], Optional[int], bool
]:
    """
    Extract (name, hand_code, pitcher_id, is_confirmed) from a probablePitcher dict.
    Returns (None, None, None, False) when no pitcher listed.
    """
    if not pitcher_dict:
        return None, None, None, False
    name = pitcher_dict.get("fullName") or pitcher_dict.get("name")
    pid = pitcher_dict.get("id")
    hand = None
    ph = pitcher_dict.get("pitchHand")
    if isinstance(ph, dict):
        hand = ph.get("code")
    elif isinstance(ph, str):
        hand = ph
    return name, hand, pid, bool(name)


def _team_name_from_dict(team_dict: dict) -> str:
    """Extract and canonicalize team name from a schedule team dict."""
    name = team_dict.get("team", {}).get("name") or team_dict.get("name") or ""
    return to_canonical(name)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class MlbProbablesScraper(BaseScraper):

    def fetch(self) -> dict:
        days_ahead = int(self.config.get("days_ahead", 2))
        today = datetime.now(timezone.utc).date()
        end_date = today + timedelta(days=days_ahead)
        params = {
            **SCHEDULE_PARAMS,
            "startDate": today.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
        }
        logger.info("Fetching MLB schedule %s → %s", today, end_date)
        resp = requests.get(MLB_SCHEDULE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if "dates" not in data:
            raise RuntimeError(
                f"MLB schedule response missing 'dates' key. "
                f"Got keys: {list(data.keys())}"
            )
        return data

    def content_key(self, raw: dict) -> str:
        games = []
        for date_entry in raw.get("dates", []):
            for game in date_entry.get("games", []):
                gid = game.get("gamePk", "")
                away_p = (
                    game.get("teams", {})
                    .get("away", {})
                    .get("probablePitcher", {})
                    .get("id")
                )
                home_p = (
                    game.get("teams", {})
                    .get("home", {})
                    .get("probablePitcher", {})
                    .get("id")
                )
                games.append(f"{gid}:{away_p}:{home_p}")
        return "|".join(sorted(games))

    def parse(self, raw: dict) -> list[dict]:
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = []
        for date_entry in raw.get("dates", []):
            date_str = date_entry.get("date", "")
            for game in date_entry.get("games", []):
                game_pk = game.get("gamePk")
                game_date = game.get("gameDate", "")
                teams = game.get("teams", {})
                away_team_str = _team_name_from_dict(teams.get("away", {}))
                home_team_str = _team_name_from_dict(teams.get("home", {}))
                away_probable = teams.get("away", {}).get("probablePitcher")
                home_probable = teams.get("home", {}).get("probablePitcher")
                away_name, away_hand, away_id, away_confirmed = _parse_pitcher(away_probable)
                home_name, home_hand, home_id, home_confirmed = _parse_pitcher(home_probable)
                record = {
                    "game_id": str(game_pk) if game_pk else f"{date_str}_{away_team_str}_{home_team_str}",
                    "date": date_str,
                    "commence_time": game_date,
                    "away_team": away_team_str,
                    "home_team": home_team_str,
                    "away_pitcher": away_name,
                    "home_pitcher": home_name,
                    "away_hand": away_hand,
                    "home_hand": home_hand,
                    "away_pitcher_id": away_id,
                    "home_pitcher_id": home_id,
                    "away_confirmed": away_confirmed,
                    "home_confirmed": home_confirmed,
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                }
                records.append(record)
        logger.info("Parsed %d MLB probable games", len(records))
        return records

    def validate(self, records: list[dict]) -> list[ProbablePitcherRecord]:
        validated = []
        for r in records:
            try:
                validated.append(ProbablePitcherRecord(**r))
            except Exception as e:
                logger.warning("Invalid probable record skipped: %s | record=%s", e, r)
        return validated

    def upsert(self, validated: list[ProbablePitcherRecord]) -> None:
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()
        payload = ProbablesSnapshot(
            generated_at=fetched_at,
            record_count=len(validated),
            updated=fetched_at,
            game_count=len(validated),
            games=validated,
        ).model_dump(mode="json")
        sm.persist_raw(source="mlb_probables", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/probables.json (%d games)", len(validated))
