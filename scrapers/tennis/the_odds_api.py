"""
scrapers/tennis/the_odds_api.py

Tennis odds scraper using The Odds API (tennis.odds endpoint).
Converts decimal odds (European format) to American moneyline format so all
odds in GCS are consistent regardless of source.

Decimal → American conversion:
  Favourite  (decimal < 2.0):  American = round((100 / (decimal - 1)) * -1)
  Underdog   (decimal >= 2.0): American = round((decimal - 1) * 100)

  Example: 1.37 decimal → -270 American (favourite)
           3.50 decimal → +250 American (underdog)
"""

import os
import requests
from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager


# ---------------------------------------------------------------------------
# Odds conversion
# ---------------------------------------------------------------------------

def decimal_to_american(decimal_odds: float) -> Optional[int]:
    """
    Convert European decimal odds to American moneyline format.

    Args:
        decimal_odds: Decimal odds value (e.g. 1.37, 3.50). Must be > 1.0.

    Returns:
        American odds as an integer (e.g. -270, +250), or None if invalid.
    """
    if decimal_odds is None:
        return None
    try:
        decimal_odds = float(decimal_odds)
        if decimal_odds <= 1.0:
            return None
        if decimal_odds < 2.0:
            return round((100 / (decimal_odds - 1)) * -1)
        else:
            return round((decimal_odds - 1) * 100)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class TennisOdds(BaseModel):
    match_id: str
    tournament: str
    p1_name: str
    p2_name: str
    # American moneyline format (converted from decimal)
    p1_ml: Optional[int] = None
    p2_ml: Optional[int] = None
    bookmaker: str
    commence_time: str


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class TheOddsApiScraper(BaseScraper):
    """
    Fetches WTA/ATP tennis match odds from The Odds API.
    Odds are returned in European decimal format and converted to American
    moneyline before writing to GCS.

    Requires env var: THE_ODDS_API_KEY
    """

    def fetch(self) -> Any:
        api_key = os.environ.get("THE_ODDS_API_KEY")
        if not api_key:
            raise ValueError("THE_ODDS_API_KEY not set")
        url = (
            f"https://api.the-odds-api.com/v4/sports/tennis/odds/"
            f"?apiKey={api_key}&regions=us&markets=h2h"
        )
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        return res.json()

    def content_key(self, raw: Any) -> Any:
        return raw

    def parse(self, raw: Any) -> List[dict]:
        odds_out = []
        for event in raw:
            book = next(
                (b for b in event.get("bookmakers", []) if b["key"] == "draftkings"),
                None,
            )
            if not book:
                continue
            h2h = next(
                (m for m in book["markets"] if m["key"] == "h2h"),
                None,
            )
            if not h2h:
                continue
            home_team = event["home_team"]
            away_team = event["away_team"]
            p1_decimal = next(
                (o["price"] for o in h2h["outcomes"] if o["name"] == home_team),
                None,
            )
            p2_decimal = next(
                (o["price"] for o in h2h["outcomes"] if o["name"] == away_team),
                None,
            )
            odds_out.append({
                "match_id": event["id"],
                "tournament": event["sport_title"],
                "p1_name": self.resolver.resolve(home_team),
                "p2_name": self.resolver.resolve(away_team),
                "p1_ml": decimal_to_american(p1_decimal),
                "p2_ml": decimal_to_american(p2_decimal),
                "bookmaker": "DraftKings",
                "commence_time": event["commence_time"],
            })
        return odds_out

    def validate(self, records: List[dict]) -> List[BaseModel]:
        return [TennisOdds(**r) for r in records]

    def upsert(self, records: List[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            # Standard envelope — schema_version 1
            "schema_version": 1,
            "generated_at": now,
            "scraper_key": "the_odds_api",
            "record_count": len(records),
            "warnings": [],  # TODO: propagate scraper warnings here
            # Existing fields — unchanged
            "updated": now,
            "odds": [r.model_dump(mode="json") for r in records],
        }
        storage.write_json(self.config["gcs_object"], payload)
