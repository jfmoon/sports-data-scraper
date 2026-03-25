"""
scrapers/sports/action_network.py

Multi-sport odds scraper using the Action Network internal API.
Covers NBA, MLB, NHL, NFL, and NCAAB (h2h moneyline, spread, total).

MLB-specific behaviour:
    When sport=mlb and days_ahead > 0, fetches today + days_ahead days
    and writes all games into a single combined snapshot. This lets the
    Transform layer join probables + lineups + weather + odds across the
    full betting window without making multiple GCS reads.

    Other sports (NBA, NHL, NFL) default to days_ahead=0 (today only)
    since their schedules are handled separately.

Architectural notes:
  - Action Network returns American moneyline odds natively — no conversion.
  - Each sport is a separate config.yaml entry pointing at this same class.
  - Team names use full_name directly — no crosswalk yet for non-CBB sports.
  - Do NOT use for tennis — Action Network tennis uses a different schema
    with no player full_name. Use the_odds_api.py for tennis.

API endpoint:
    GET https://api.actionnetwork.com/web/v2/scoreboard/{sport}
        ?bookIds=15&date=YYYYMMDD&periods=event

Known working sport slugs: ncaab, nba, mlb, nhl, nfl, ncaaf, soccer
"""

import logging
from datetime import datetime, timezone, timedelta, date
from typing import Optional

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager

logger = logging.getLogger(__name__)

ACTION_NETWORK_URL = "https://api.actionnetwork.com/web/v2/scoreboard/{sport}"


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class GameOdds(BaseModel):
    game_id: int
    sport: str
    status: str
    date: str                            # YYYY-MM-DD — which day this game is on
    commence_time: str                   # ISO string
    away_team: str
    home_team: str
    bookmaker: str
    away_ml: Optional[int] = None
    home_ml: Optional[int] = None
    away_spread: Optional[float] = None
    away_spread_odds: Optional[int] = None
    home_spread: Optional[float] = None
    home_spread_odds: Optional[int] = None
    total: Optional[float] = None
    over_odds: Optional[int] = None
    under_odds: Optional[int] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_odds(markets: dict, book_id: int, away_team_id: int, home_team_id: int) -> dict:
    """Extract flat odds dict from a game's nested markets block."""
    result = {
        "away_ml": None, "home_ml": None,
        "away_spread": None, "away_spread_odds": None,
        "home_spread": None, "home_spread_odds": None,
        "total": None, "over_odds": None, "under_odds": None,
    }

    book_data = markets.get(str(book_id), {}).get("event", {})
    if not book_data:
        return result

    # Moneyline
    for o in book_data.get("moneyline", []):
        tid = o.get("team_id")
        odds = o.get("odds")
        if odds is None:
            continue
        if tid == away_team_id:
            result["away_ml"] = int(odds)
        elif tid == home_team_id:
            result["home_ml"] = int(odds)

    # Spread
    for o in book_data.get("spread", []):
        tid = o.get("team_id")
        val = o.get("value")
        odds = o.get("odds")
        if val is None or odds is None:
            continue
        if tid == away_team_id:
            result["away_spread"] = float(val)
            result["away_spread_odds"] = int(odds)
        elif tid == home_team_id:
            result["home_spread"] = float(val)
            result["home_spread_odds"] = int(odds)

    # Total
    for o in book_data.get("total", []):
        side = o.get("side", "").lower()
        val = o.get("value")
        odds = o.get("odds")
        if val is None or odds is None:
            continue
        if side == "over":
            result["total"] = float(val)
            result["over_odds"] = int(odds)
        elif side == "under":
            result["under_odds"] = int(odds)

    return result


def _fetch_one_date(sport: str, fetch_date: date, book_id: int) -> list[dict]:
    """
    Fetch all games for a single sport + date from Action Network.
    Returns list of raw game dicts, empty list on failure.
    """
    url = ACTION_NETWORK_URL.format(sport=sport)
    params = {
        "bookIds": str(book_id),
        "date": fetch_date.strftime("%Y%m%d"),
        "periods": "event",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; sports-data-scraper/1.0; "
            "+https://github.com/jfmoon/sports-data-scraper)"
        ),
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("Action Network fetch failed for %s date=%s: %s", sport, fetch_date, e)
        return []

    games = data.get("games", [])
    if not games:
        logger.info("Action Network: no games for %s on %s", sport, fetch_date)

    # Tag each game with the date string so we can include it in the record
    for g in games:
        g["_fetch_date"] = fetch_date.strftime("%Y-%m-%d")

    return games


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class ActionNetworkOddsScraper(BaseScraper):
    """
    Fetches game odds from Action Network for one sport across a date window.

    For MLB: fetches today + days_ahead days (default 2) and combines all
    games into a single snapshot. This aligns with the mlb_probables window
    so odds, probables, and weather all cover the same date range.

    For other sports: days_ahead defaults to 0 (today only) unless overridden
    in config.yaml.
    """

    def _get_sport(self) -> str:
        return self.config["sport"]

    def _get_book_id(self) -> int:
        return int(self.config.get("primary_book_id", 15))

    def _get_days_ahead(self) -> int:
        # MLB defaults to 2 to match probables/weather/lineups window.
        # Other sports default to 0 (today only).
        sport = self._get_sport()
        default = 2 if sport == "mlb" else 0
        return int(self.config.get("days_ahead", default))

    def fetch(self) -> dict:
        sport = self._get_sport()
        book_id = self._get_book_id()
        days_ahead = self._get_days_ahead()
        today = datetime.now(timezone.utc).date()

        all_games = []
        dates_fetched = []

        for offset in range(days_ahead + 1):
            fetch_date = today + timedelta(days=offset)
            dates_fetched.append(str(fetch_date))
            games = _fetch_one_date(sport, fetch_date, book_id)
            all_games.extend(games)
            logger.info(
                "Action Network %s: %d games on %s", sport, len(games), fetch_date
            )

        logger.info(
            "Action Network %s: %d total games across %s",
            sport, len(all_games), dates_fetched
        )
        return {
            "sport": sport,
            "book_id": book_id,
            "dates": dates_fetched,
            "games": all_games,
        }

    def content_key(self, raw: dict) -> str:
        """Hash on game IDs + moneylines across all dates — excludes timestamps."""
        book_id = str(raw.get("book_id", "15"))
        parts = []
        for g in raw.get("games", []):
            gid = g.get("id", "")
            away_id = g.get("away_team_id", "")
            home_id = g.get("home_team_id", "")
            odds = _extract_odds(g.get("markets", {}), int(book_id), away_id, home_id)
            parts.append(f"{gid}:{odds['away_ml']}:{odds['home_ml']}")
        return "|".join(sorted(parts))

    def parse(self, raw: dict) -> list[dict]:
        sport = raw["sport"]
        book_id = raw["book_id"]
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = []

        for g in raw.get("games", []):
            gid = g.get("id")
            if not gid:
                continue

            status = g.get("status", "")
            commence_time = g.get("start_time", "")
            fetch_date = g.get("_fetch_date", "")

            # Build team id → name lookup from the game's teams list
            teams = {t["id"]: t.get("full_name", "") for t in g.get("teams", [])}
            away_id = g.get("away_team_id")
            home_id = g.get("home_team_id")
            away_team = teams.get(away_id, "")
            home_team = teams.get(home_id, "")

            if not away_team or not home_team:
                logger.warning(
                    "Action Network %s: skipping game %s — missing team names", sport, gid
                )
                continue

            odds = _extract_odds(g.get("markets", {}), book_id, away_id, home_id)

            # Skip games with no odds at all — nothing useful for analysis
            if odds["away_ml"] is None and odds["home_ml"] is None:
                logger.debug(
                    "Action Network %s: skipping game %s — no moneyline for book %s",
                    sport, gid, book_id
                )
                continue

            records.append({
                "game_id": gid,
                "sport": sport,
                "status": status,
                "date": fetch_date,
                "commence_time": commence_time,
                "away_team": away_team,
                "home_team": home_team,
                "bookmaker": f"ActionNetwork:{book_id}",
                **odds,
            })

        logger.info(
            "Parsed %d %s games with odds across %s",
            len(records), sport, raw.get("dates", [])
        )
        return records

    def validate(self, records: list[dict]) -> list[GameOdds]:
        validated = []
        for r in records:
            try:
                validated.append(GameOdds(**r))
            except Exception as e:
                logger.warning(
                    "Invalid GameOdds record: %s | game=%s", e, r.get("game_id")
                )
        return validated

    def upsert(self, validated: list[GameOdds]) -> None:
        sport = self._get_sport()
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()

        payload = {
            "updated": fetched_at,
            "sport": sport,
            "dates": self.config.get("days_ahead", 0),
            "game_count": len(validated),
            "odds": [g.model_dump(mode="json") for g in validated],
        }

        sm.persist_raw(source=f"{sport}_odds", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info(
            "Wrote %s (%d games)", self.config["gcs_object"], len(validated)
        )
