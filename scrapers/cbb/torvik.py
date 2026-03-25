"""
scrapers/cbb/torvik.py

Framework wrapper for the Bart Torvik standalone scraper.
Integrates with run.py / ScraperRunner via BaseScraper.

BaseScraper contract (base/scraper.py):
  fetch()           -> Any             (raw data from source)
  content_key(raw)  -> Any             (slice for dedup hashing)
  parse(raw)        -> List[dict]      (normalized dicts)
  validate(records) -> List[BaseModel] (Pydantic validation)
  upsert(records)   -> None            (write to GCS)

Architectural constraints:
  - Extract layer only. No scoring, normalization, or cross-record comparisons.
  - Do not modify base/, storage.py, or run.py.
  - GCS paths are defined in config.yaml; read from self.config here.
"""

import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.cbb.torvik_scraper import scrape_torvik

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic model for a single Torvik team record
# ---------------------------------------------------------------------------

class TorvikTeam(BaseModel):
    name: str
    rank: Optional[int]
    adj_o: Optional[float]
    adj_d: Optional[float]
    adj_t: Optional[float]
    t_rank: Optional[float]
    wab: Optional[float]


# ---------------------------------------------------------------------------
# Wrapper
# ---------------------------------------------------------------------------

class TorvikScraper(BaseScraper):
    """
    Scrapes Bart Torvik team ratings for full-season and last-10-games splits.

    GCS outputs (configured in config.yaml):
      cbb/torvik_team.json  — full-season snapshot
      cbb/torvik_splits.json — both splits with metadata
    """

    def fetch(self) -> dict:
        """
        Run the standalone scraper for each configured split.

        Returns:
            {
                "full_season": {"normalized_data": [...], "metadata": {...}},
                "last_10":     {"normalized_data": [...], "metadata": {...}},
            }

        Raises:
            ValueError: On header drift or any unresolved team name.
                        Halts before any downstream processing.
        """
        splits = self.config.get("split_presets", ["full_season", "last_10"])
        results = {}
        for split in splits:
            logger.info("Torvik: scraping split '%s'", split)
            results[split] = scrape_torvik(split, year=self.config.get("year", 2026))
            logger.info(
                "Torvik: split '%s' complete — %d teams",
                split,
                results[split]["metadata"]["team_count"],
            )
        return results

    def content_key(self, raw: dict) -> Any:
        """
        Return the full-season team list for dedup hashing.
        Excludes timestamps so unchanged data is skipped correctly.
        """
        return raw.get("full_season", {}).get("normalized_data", [])

    def parse(self, raw: dict) -> List[dict]:
        """
        Extract the full-season normalized team list.
        The standalone scraper already returns clean, canonical dicts —
        no additional parsing needed here.
        """
        return raw.get("full_season", {}).get("normalized_data", [])

    def validate(self, records: List[dict]) -> List[BaseModel]:
        """
        Run Pydantic validation on each team record and check completeness.
        Returns a list of TorvikTeam models.

        Raises:
            ValueError: If fewer than 350 teams passed (structural failure).
        """
        if len(records) < 350:
            raise ValueError(
                f"Torvik validate: only {len(records)} teams — expected >= 350. "
                f"Aborting before GCS write."
            )
        validated = []
        errors = 0
        for record in records:
            try:
                validated.append(TorvikTeam(**record))
            except Exception as e:
                logger.warning("Torvik: validation failed for record %s: %s", record, e)
                errors += 1
        if errors > 0:
            logger.warning("Torvik: %d/%d records failed Pydantic validation", errors, len(records))
        return validated

    def upsert(self, records: List[BaseModel]) -> None:
        """
        Write the full-season GCS snapshot from the validated records.
        """
        teams = [r.model_dump() for r in records]
        storage = StorageManager(self.config["bucket"])
        now = datetime.now(timezone.utc).isoformat()

        storage.write_json(
            self.config.get("gcs_object", "cbb/torvik_team.json"),
            {
                # Standard envelope — schema_version 1
                "schema_version": 1,
                "generated_at": now,
                "scraper_key": "torvik",
                "record_count": len(teams),
                "warnings": [],  # TODO: propagate scraper warnings here
                # Existing fields — unchanged
                "updated": now,
                "team_count": len(teams),
                "teams": teams,
            },
        )
        logger.info(
            "Torvik: wrote %d teams to %s",
            len(teams),
            self.config.get("gcs_object", "cbb/torvik_team.json"),
        )
