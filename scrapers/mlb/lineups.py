"""
scrapers/mlb/lineups.py

Scrapes projected/confirmed MLB starting lineups from the MLB Stats API.
Output: mlb/lineups.json
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

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
MLB_GAME_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
SOURCE = "mlb_stats_api"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LineupSlot(BaseModel):
    batting_order: int
    player_name: str
    player_id: Optional[int]
    position: Optional[str]
    bats: Optional[str]

    @field_validator("bats", mode="before")
    @classmethod
    def normalize_bats(cls, v):
        if v is None:
            return None
        v = str(v).strip().upper()
        return v if v in ("L", "R", "S", "B") else None


class LineupGame(BaseModel):
    game_id: str
    date: str
    commence_time: str
    away_team: str
    home_team: str
    away_confirmed: bool
    home_confirmed: bool
    away_lineup: list[LineupSlot]
    home_lineup: list[LineupSlot]
    source: str
    fetched_at: str


class LineupsSnapshot(BaseModel):
    # Standard envelope — schema_version 1
    schema_version: int = 1
    generated_at: str
    scraper_key: str = "mlb_lineups"
    record_count: int
    warnings: list[str] = []
    # Existing fields — unchanged
    updated: str
    game_count: int
    games: list[LineupGame]


# ---------------------------------------------------------------------------
# Provider: MLB Stats API live feed
# ---------------------------------------------------------------------------

def _fetch_game_pks(days_ahead: int) -> list[tuple[str, str, str]]:
    today = datetime.now(timezone.utc).date()
    end_date = today + timedelta(days=days_ahead)
    params = {
        "sportId": 1,
        "startDate": today.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "fields": "dates,date,games,gamePk,gameDate",
    }
    resp = requests.get(MLB_SCHEDULE_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_entry in data.get("dates", []):
        date_str = date_entry.get("date", "")
        for game in date_entry.get("games", []):
            pk = game.get("gamePk")
            if pk:
                games.append((str(pk), date_str, game.get("gameDate", "")))
    return games


def _fetch_lineup_for_game(game_pk: str) -> dict:
    url = MLB_GAME_FEED_URL.format(game_pk=game_pk)
    resp = requests.get(url, timeout=15)
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    return resp.json()


def _parse_lineup_side(boxscore_side: dict) -> tuple[list[dict], bool]:
    players = boxscore_side.get("players", {})
    if not players:
        return [], False
    slots = []
    for _, player_data in players.items():
        batting_order = player_data.get("battingOrder")
        if not batting_order:
            continue
        try:
            order_int = int(batting_order)
            if order_int % 100 != 0:
                continue
            batting_pos = order_int // 100
        except (ValueError, TypeError):
            continue
        person = player_data.get("person", {})
        pos = player_data.get("position", {}).get("abbreviation")
        bat_side = player_data.get("batSide", {})
        bats_code = bat_side.get("code") if isinstance(bat_side, dict) else None
        slots.append({
            "batting_order": batting_pos,
            "player_name": person.get("fullName") or "",
            "player_id": person.get("id"),
            "position": pos,
            "bats": bats_code,
        })
    slots.sort(key=lambda s: s["batting_order"])
    confirmed = len(slots) > 0
    return slots, confirmed


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class LineupsScraper(BaseScraper):

    def fetch(self) -> dict:
        days_ahead = int(self.config.get("days_ahead", 1))
        game_pks = _fetch_game_pks(days_ahead)
        logger.info("Fetching lineups for %d scheduled games", len(game_pks))
        results = []
        for game_pk, date_str, commence_time in game_pks:
            try:
                feed = _fetch_lineup_for_game(game_pk)
                results.append({
                    "game_pk": game_pk, "date": date_str,
                    "commence_time": commence_time, "feed": feed,
                })
            except Exception as e:
                logger.warning("Failed to fetch lineup for game %s: %s", game_pk, e)
                results.append({
                    "game_pk": game_pk, "date": date_str,
                    "commence_time": commence_time, "feed": {},
                })
        return {"games": results}

    def content_key(self, raw: dict) -> str:
        parts = []
        for g in raw.get("games", []):
            game_pk = g["game_pk"]
            feed = g.get("feed", {})
            boxscore = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})

            def _ordered_ids(side_dict: dict) -> str:
                players = side_dict.get("players", {})
                starters = []
                for _, pd in players.items():
                    bo = pd.get("battingOrder")
                    if not bo:
                        continue
                    try:
                        order_int = int(bo)
                        if order_int % 100 != 0:
                            continue
                        pid = pd.get("person", {}).get("id", "")
                        starters.append((order_int, str(pid)))
                    except (ValueError, TypeError):
                        continue
                starters.sort(key=lambda x: x[0])
                return ",".join(pid for _, pid in starters)

            away_ids = _ordered_ids(boxscore.get("away", {}))
            home_ids = _ordered_ids(boxscore.get("home", {}))
            parts.append(f"{game_pk}|{away_ids}|{home_ids}")
        return "||".join(sorted(parts))

    def parse(self, raw: dict) -> list[dict]:
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = []
        for g in raw.get("games", []):
            game_pk = g["game_pk"]
            date_str = g["date"]
            commence_time = g["commence_time"]
            feed = g.get("feed", {})
            game_data = feed.get("gameData", {})
            teams_data = game_data.get("teams", {})
            away_name_raw = teams_data.get("away", {}).get("name", "")
            home_name_raw = teams_data.get("home", {}).get("name", "")
            away_team = to_canonical(away_name_raw) if away_name_raw else ""
            home_team = to_canonical(home_name_raw) if home_name_raw else ""
            boxscore = feed.get("liveData", {}).get("boxscore", {}).get("teams", {})
            away_side = boxscore.get("away", {})
            home_side = boxscore.get("home", {})
            away_slots, away_confirmed = _parse_lineup_side(away_side)
            home_slots, home_confirmed = _parse_lineup_side(home_side)
            records.append({
                "game_id": game_pk, "date": date_str, "commence_time": commence_time,
                "away_team": away_team, "home_team": home_team,
                "away_confirmed": away_confirmed, "home_confirmed": home_confirmed,
                "away_lineup": away_slots, "home_lineup": home_slots,
                "source": SOURCE, "fetched_at": fetched_at,
            })
        logger.info("Parsed %d lineup records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[LineupGame]:
        validated = []
        for r in records:
            try:
                validated.append(LineupGame(**r))
            except Exception as e:
                logger.warning(
                    "Invalid lineup record: %s | game=%s %s@%s",
                    e, r.get("game_id"), r.get("away_team"), r.get("home_team")
                )
        return validated

    def upsert(self, validated: list[LineupGame]) -> None:
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()
        payload = LineupsSnapshot(
            generated_at=fetched_at,
            record_count=len(validated),
            updated=fetched_at,
            game_count=len(validated),
            games=validated,
        ).model_dump(mode="json")
        sm.persist_raw(source="mlb_lineups", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/lineups.json (%d games)", len(validated))
