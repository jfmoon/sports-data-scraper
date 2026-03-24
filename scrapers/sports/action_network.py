"""
scrapers/sports/action_network.py

Multi-sport odds scraper using the Action Network internal API.
Covers NBA, MLB, NHL, NFL, and NCAAB (h2h moneyline, spread, total).

Architectural notes:
  - Action Network returns American moneyline odds natively — no conversion needed.
  - Each sport is a separate config.yaml entry pointing at this same class.
  - Team names use full_name (e.g. "Charlotte Hornets") — map via to_canonical()
    once crosswalks for each sport are added to data/crosswalks/.
  - Do NOT use this scraper for tennis — Action Network's tennis schema uses
    "competitions"/"competitors" with no player full_name. Use the_odds_api.py
    for tennis odds.

API endpoint:
    GET https://api.actionnetwork.com/web/v2/scoreboard/{sport}
        ?bookIds=15,30,79,2988,75,123,71,68,69,4727
        &date=YYYYMMDD
        &periods=event

Known working sport slugs:
    ncaab, nba, mlb, nhl, nfl, ncaaf, soccer

Book IDs (DraftKings = 15, FanDuel = 30, BetMGM = 79):
    Configured per scraper entry in config.yaml via `book_ids` list.
    Defaults to DraftKings (15) if not specified.
"""

import os
import requests
from datetime import datetime, timezone, date
from typing import Any, List, Optional

from pydantic import BaseModel
from base.scraper import BaseScraper
from base.storage import StorageManager


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class GameOdds(BaseModel):
    game_id: int
    sport: str
    status: str                          # "scheduled", "in_progress", "complete"
    commence_time: str                   # ISO string
    away_team: str
    home_team: str
    bookmaker: str
    # Moneyline (American format)
    away_ml: Optional[int] = None
    home_ml: Optional[int] = None
    # Spread
    away_spread: Optional[float] = None
    away_spread_odds: Optional[int] = None
    home_spread: Optional[float] = None
    home_spread_odds: Optional[int] = None
    # Total
    total: Optional[float] = None
    over_odds: Optional[int] = None
    under_odds: Optional[int] = None


# ---------------------------------------------------------------------------
# Helper: extract odds from a market block
# ---------------------------------------------------------------------------

def _extract_odds(markets: dict, book_id: int, away_team_id: int, home_team_id: int) -> dict:
    """
    Parse the nested markets dict for a single game and return a flat odds dict.

    markets structure:
        {book_id_str: {"event": {"moneyline": [...], "spread": [...], "total": [...]}}}

    Moneyline outcomes have `team_id` and `side` ("away"/"home").
    Spread outcomes have `team_id`, `value` (points), and `odds`.
    Total outcomes have `side` ("over"/"under"), `value`, and `odds`.
    """
    result = {}
    book_data = markets.get(str(book_id), {}).get("event", {})

    # Moneyline
    for outcome in book_data.get("moneyline", []):
        if outcome.get("team_id") == away_team_id:
            result["away_ml"] = outcome.get("odds")
        elif outcome.get("team_id") == home_team_id:
            result["home_ml"] = outcome.get("odds")

    # Spread
    for outcome in book_data.get("spread", []):
        if outcome.get("team_id") == away_team_id:
            result["away_spread"] = outcome.get("value")
            result["away_spread_odds"] = outcome.get("odds")
        elif outcome.get("team_id") == home_team_id:
            result["home_spread"] = outcome.get("value")
            result["home_spread_odds"] = outcome.get("odds")

    # Total
    for outcome in book_data.get("total", []):
        side = outcome.get("side")
        if side == "over":
            result["total"] = outcome.get("value")
            result["over_odds"] = outcome.get("odds")
        elif side == "under":
            result["under_odds"] = outcome.get("odds")

    return result


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class ActionNetworkOddsScraper(BaseScraper):
    """
    Fetches game odds for a configured sport from the Action Network API.

    config.yaml keys (under this scraper's entry):
        sport       - Action Network sport slug (e.g. "nba", "mlb", "nhl", "nfl", "ncaab")
        book_ids    - list of book IDs to request (default: [15] = DraftKings)
        primary_book_id - which book to extract odds from (default: 15 = DraftKings)
        gcs_object  - GCS path for the snapshot (e.g. "nba/odds.json")
        bucket      - GCS bucket name

    Odds format: American moneyline (e.g. -110, +250).
    Action Network returns American odds natively — no conversion needed.
    """

    # Default book IDs to request from the API (broader = better fallback coverage)
    DEFAULT_BOOK_IDS = [15, 30, 79, 2988, 75, 123, 71, 68, 69, 4727]

    def fetch(self) -> dict:
        sport = self.config.get("sport")
        if not sport:
            raise ValueError("ActionNetworkOddsScraper: 'sport' must be set in config.yaml")

        book_ids = self.config.get("book_ids", self.DEFAULT_BOOK_IDS)
        book_ids_str = ",".join(str(b) for b in book_ids)
        today = date.today().strftime("%Y%m%d")

        url = (
            f"https://api.actionnetwork.com/web/v2/scoreboard/{sport}"
            f"?bookIds={book_ids_str}&date={today}&periods=event"
        )

        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if "games" not in data:
            raise ValueError(
                f"ActionNetworkOddsScraper: unexpected response for sport '{sport}'. "
                f"Top-level keys: {list(data.keys())}. "
                f"Note: tennis uses a different schema — use the_odds_api.py for tennis."
            )

        return data

    def content_key(self, raw: dict) -> Any:
        # Hash on game IDs + odds snapshot — exclude timestamps
        return [
            {"id": g["id"], "markets": g.get("markets", {})}
            for g in raw.get("games", [])
        ]

    def parse(self, raw: dict) -> List[dict]:
        sport = self.config.get("sport", "unknown")
        primary_book = self.config.get("primary_book_id", 15)
        results = []

        for game in raw.get("games", []):
            away_id = game.get("away_team_id")
            home_id = game.get("home_team_id")

            # Resolve team names from the embedded teams list
            team_map = {t["id"]: t.get("full_name", "") for t in game.get("teams", [])}
            away_name = team_map.get(away_id, f"team_{away_id}")
            home_name = team_map.get(home_id, f"team_{home_id}")

            markets = game.get("markets", {})

            # Skip games with no odds for the primary book
            if not markets.get(str(primary_book), {}).get("event"):
                continue

            odds = _extract_odds(markets, primary_book, away_id, home_id)

            results.append({
                "game_id":       game["id"],
                "sport":         sport,
                "status":        game.get("status", ""),
                "commence_time": game.get("start_time", ""),
                "away_team":     away_name,
                "home_team":     home_name,
                "bookmaker":     f"ActionNetwork:{primary_book}",
                **odds,
            })

        return results

    def validate(self, records: List[dict]) -> List[BaseModel]:
        return [GameOdds(**r) for r in records]

    def upsert(self, records: List[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        payload = {
            "updated":    datetime.now(timezone.utc).isoformat(),
            "sport":      self.config.get("sport"),
            "game_count": len(records),
            "odds":       [r.model_dump(mode="json") for r in records],
        }
        storage.write_json(self.config["gcs_object"], payload)
