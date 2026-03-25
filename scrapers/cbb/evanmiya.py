"""
scrapers/cbb/evanmiya.py

EvanMiya framework wrapper.
Integrates evanmiya_scraper.py into the repo's BaseScraper / ScraperRunner system.

Extraction logic lives in evanmiya_scraper.py. This file handles:
  - BaseScraper contract (fetch / content_key / parse / validate / upsert)
  - Pydantic model definition (EvanMiyaTeam)
  - GCS write via StorageManager

The scraper opens a visible browser and waits for manual login. It cannot run
autonomously on Cloud Run without Playwright + Chromium container setup and
session-cookie injection.
"""

import json
import hashlib
from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.cbb.evanmiya_scraper import scrape_evanmiya, build_content_key


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class EvanMiyaTeam(BaseModel):
    # Required fields — always present when scrape succeeds
    name: str
    rank: Optional[int] = None
    o_rate: Optional[float] = None
    d_rate: Optional[float] = None
    relative_rating: Optional[float] = None
    # Available in the authenticated full table
    opp_adjust: Optional[float] = None
    roster_rank: Optional[int] = None
    # Optional — present if the live table includes these columns
    pace_adjust: Optional[float] = None
    off_rank: Optional[int] = None
    def_rank: Optional[int] = None
    true_tempo: Optional[float] = None
    tempo_rank: Optional[int] = None
    injury_rank: Optional[int] = None
    home_rank: Optional[int] = None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class EvanMiyaScraper(BaseScraper):
    """
    Scrapes EvanMiya team ratings via Playwright DOM extraction.

    config.yaml keys used:
      bucket      – GCS bucket name
      gcs_object  – GCS object path (e.g. "cbb/evanmiya.json")

    Requires:
      playwright installed and chromium downloaded
      Active EvanMiya subscription + manual browser login
    """

    def fetch(self) -> List[dict]:
        """
        Navigate to EvanMiya, restore saved session (or prompt for manual login
        on first run), and extract the full team ratings table.
        Returns list of normalized team dicts.
        """
        force_login = self.config.get("force_login", False)
        return scrape_evanmiya(visible=True, force_login=force_login)

    def content_key(self, raw: List[dict]) -> Any:
        """
        SHA-256 hash over sorted normalized records.
        Deterministic; excludes timestamps and metadata.
        """
        return build_content_key(raw)

    def parse(self, raw: List[dict]) -> List[dict]:
        """
        No additional parsing needed — evanmiya_scraper.py returns fully
        normalized dicts ready for validation.
        """
        return raw

    def validate(self, records: List[dict]) -> List[EvanMiyaTeam]:
        return [EvanMiyaTeam(**r) for r in records]

    def upsert(self, records: List[EvanMiyaTeam]) -> None:
        storage = StorageManager(self.config["bucket"])
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            # Standard envelope — schema_version 1
            "schema_version": 1,
            "generated_at": now,
            "scraper_key": "evanmiya",
            "record_count": len(records),
            "warnings": [],  # TODO: propagate scraper warnings here
            # Existing fields — unchanged
            "updated": now,
            "team_count": len(records),
            "teams": [r.model_dump(mode="json") for r in records],
        }
        storage.write_json(self.config["gcs_object"], payload)
