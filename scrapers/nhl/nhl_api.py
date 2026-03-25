"""
scrapers/nhl/nhl_api.py

Official NHL Stats API scraper.
Produces: nhl/schedule.json, nhl/standings.json
API base: https://api-web.nhle.com/v1 (public, no auth)

Review history:
  Round 1 (Gemini): initial implementation
  Round 2 (Claude): decouple standings failure from schedule write (Bug 4)
  Round 3 (both): catch ValueError from malformed standings JSON (remaining gap)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.nhl.names import to_canonical, make_join_key

logger = logging.getLogger(__name__)

NHL_API_BASE = "https://api-web.nhle.com/v1"
DEFAULT_TIMEOUT = 20
USER_AGENT = "sports-data-scraper/1.0 (github.com/jfmoon/sports-data-scraper)"
GAME_TYPE_REGULAR = 2
GAME_TYPE_PLAYOFF = 3


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class NhlGame(BaseModel):
    game_id: int
    season: str
    game_type: int
    game_type_label: str
    date: str
    commence_time: str
    away_team: str
    home_team: str
    join_key: str
    venue: str | None
    status: str
    away_score: int | None = None
    home_score: int | None = None
    period: int | None = None
    period_time_remaining: str | None = None
    source: str = "nhl_stats_api"
    source_url: str = ""
    fetched_at: str = ""


class NhlStandingsTeam(BaseModel):
    team: str
    conference: str | None = None
    division: str | None = None
    wins: int
    losses: int
    ot_losses: int
    points: int
    games_played: int
    regulation_wins: int | None = None
    goal_diff: int | None = None
    streak: str | None = None
    clinch_indicator: str | None = None
    source: str = "nhl_stats_api"
    fetched_at: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class NhlApiScraper(BaseScraper):

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        return s

    def _fetched_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _game_date_range(self) -> tuple[str, str]:
        days_ahead = int(self.config.get("days_ahead", 2))
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=days_ahead)
        return today.isoformat(), end.isoformat()

    def fetch(self) -> dict:
        start_date, end_date = self._game_date_range()
        session = self._session()
        schedule_url = f"{NHL_API_BASE}/schedule/{start_date}"
        standings_url = f"{NHL_API_BASE}/standings/now"

        logger.info("NHL API: fetching schedule %s → %s", start_date, end_date)
        sched_resp = session.get(schedule_url, timeout=DEFAULT_TIMEOUT)
        sched_resp.raise_for_status()
        schedule_raw = sched_resp.json()

        standings_raw: dict = {"standings": []}
        standings_ok = False
        try:
            logger.info("NHL API: fetching standings")
            stand_resp = session.get(standings_url, timeout=DEFAULT_TIMEOUT)
            stand_resp.raise_for_status()
            standings_raw = stand_resp.json()
            standings_ok = bool(standings_raw.get("standings"))
        except (requests.RequestException, ValueError) as exc:
            logger.error(
                "NHL API: standings fetch failed — schedule will still be written. "
                "Error: %s: %s", type(exc).__name__, exc
            )

        return {
            "schedule": schedule_raw,
            "standings": standings_raw,
            "meta": {
                "start_date": start_date,
                "end_date": end_date,
                "schedule_url": schedule_url,
                "standings_url": standings_url,
                "standings_ok": standings_ok,
            },
        }

    def content_key(self, raw: dict) -> Any:
        games = []
        for week in raw["schedule"].get("gameWeek", []):
            for g in week.get("games", []):
                games.append((
                    g.get("id"),
                    g.get("gameState"),
                    g.get("awayTeam", {}).get("score"),
                    g.get("homeTeam", {}).get("score"),
                ))
        return games

    def _parse_game_status(self, state: str | None) -> str:
        if not state:
            return "unknown"
        state = state.upper()
        if state in ("FUT", "PRE"):
            return "scheduled"
        if state in ("LIVE", "CRIT"):
            return "live"
        if state in ("OFF", "FINAL"):
            return "final"
        if state == "PPD":
            return "postponed"
        return "unknown"

    def _parse_game_type(self, gtype: int) -> str:
        if gtype == GAME_TYPE_REGULAR:
            return "regular"
        if gtype == GAME_TYPE_PLAYOFF:
            return "playoff"
        return f"type_{gtype}"

    def parse(self, raw: dict) -> list[dict]:
        fetched_at = self._fetched_at()
        end_date = raw["meta"]["end_date"]
        schedule_url = raw["meta"]["schedule_url"]
        games_out: list[dict] = []

        for week in raw["schedule"].get("gameWeek", []):
            game_date = week.get("date", "")
            if game_date and game_date > end_date:
                continue
            for g in week.get("games", []):
                game_type = g.get("gameType", 2)
                if game_type == 1 and not self.config.get("include_preseason", False):
                    continue
                away_raw = (
                    g.get("awayTeam", {}).get("placeName", {}).get("default", "")
                    or g.get("awayTeam", {}).get("teamName", {}).get("default", "")
                )
                home_raw = (
                    g.get("homeTeam", {}).get("placeName", {}).get("default", "")
                    or g.get("homeTeam", {}).get("teamName", {}).get("default", "")
                )
                away_full = g.get("awayTeam", {}).get("commonName", {}).get("default") or away_raw
                home_full = g.get("homeTeam", {}).get("commonName", {}).get("default") or home_raw
                away_team = to_canonical(away_full) if away_full else away_full
                home_team = to_canonical(home_full) if home_full else home_full
                games_out.append({
                    "game_id": g.get("id"),
                    "season": str(g.get("season", "")),
                    "game_type": game_type,
                    "game_type_label": self._parse_game_type(game_type),
                    "date": game_date,
                    "commence_time": g.get("startTimeUTC", ""),
                    "away_team": away_team,
                    "home_team": home_team,
                    "join_key": make_join_key(game_date, away_team, home_team),
                    "venue": g.get("venue", {}).get("default"),
                    "status": self._parse_game_status(g.get("gameState")),
                    "away_score": g.get("awayTeam", {}).get("score"),
                    "home_score": g.get("homeTeam", {}).get("score"),
                    "period": g.get("periodDescriptor", {}).get("number"),
                    "period_time_remaining": g.get("clock", {}).get("timeRemaining"),
                    "source": "nhl_stats_api",
                    "source_url": schedule_url,
                    "fetched_at": fetched_at,
                })

        standings_out: list[dict] = []
        for team_s in raw["standings"].get("standings", []):
            team_name_raw = (
                team_s.get("teamName", {}).get("default", "")
                or team_s.get("placeName", {}).get("default", "")
            )
            team_name = to_canonical(team_name_raw) if team_name_raw else team_name_raw
            standings_out.append({
                "team": team_name,
                "conference": team_s.get("conferenceName"),
                "division": team_s.get("divisionName"),
                "wins": team_s.get("wins", 0),
                "losses": team_s.get("losses", 0),
                "ot_losses": team_s.get("otLosses", 0),
                "points": team_s.get("points", 0),
                "games_played": team_s.get("gamesPlayed", 0),
                "regulation_wins": team_s.get("regulationWins"),
                "goal_diff": team_s.get("goalDifferential"),
                "streak": team_s.get("streakCode"),
                "clinch_indicator": team_s.get("clinchIndicator"),
                "source": "nhl_stats_api",
                "fetched_at": fetched_at,
            })

        return [
            {"_type": "games", "records": games_out},
            {"_type": "standings", "records": standings_out},
        ]

    def validate(self, records: list[dict]) -> list[BaseModel]:
        validated: list[BaseModel] = []
        for batch in records:
            if batch["_type"] == "games":
                for r in batch["records"]:
                    validated.append(NhlGame(**r))
            elif batch["_type"] == "standings":
                for r in batch["records"]:
                    validated.append(NhlStandingsTeam(**r))
        return validated

    def upsert(self, records: list[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        fetched_at = self._fetched_at()

        games = [r.model_dump() for r in records if isinstance(r, NhlGame)]
        standings = [r.model_dump() for r in records if isinstance(r, NhlStandingsTeam)]

        schedule_payload = {
            # Standard envelope — schema_version 1
            "schema_version": 1,
            "generated_at": fetched_at,
            "scraper_key": "nhl_api",
            "record_count": len(games),
            "warnings": [],  # TODO: propagate scraper warnings here
            # Existing fields — unchanged
            "updated": fetched_at,
            "game_count": len(games),
            "games": games,
        }
        storage.persist_raw(source="nhl_api", data=schedule_payload)
        storage.write_json(self.config["gcs_object"], schedule_payload)
        logger.info("NHL API: wrote %d games to %s", len(games), self.config["gcs_object"])

        standings_gcs = self.config.get("standings_gcs_object", "nhl/standings.json")
        if standings:
            standings_payload = {
                # Standard envelope — schema_version 1
                "schema_version": 1,
                "generated_at": fetched_at,
                "scraper_key": "nhl_api",
                "record_count": len(standings),
                "warnings": [],  # TODO: propagate scraper warnings here
                # Existing fields — unchanged
                "updated": fetched_at,
                "team_count": len(standings),
                "standings": standings,
            }
            storage.persist_raw(source="nhl_standings", data=standings_payload)
            storage.write_json(standings_gcs, standings_payload)
            logger.info("NHL API: wrote %d standings rows to %s", len(standings), standings_gcs)
        else:
            logger.warning(
                "NHL API: standings empty — skipping standings write "
                "(standings fetch may have failed or returned empty)"
            )
