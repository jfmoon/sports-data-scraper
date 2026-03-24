"""
scrapers/cbb/torvik.py

Framework wrapper for the Bart Torvik standalone scraper.
Integrates with run.py / ScraperRunner via BaseScraper.

Architectural constraints (see AGENTS.md / sports-data-scraper-summary.md):
  - This is the Extract layer only. No scoring, normalization, or cross-record
    comparisons. Null-rate analysis belongs in jfmoon/sports-analysis.
  - Do not modify base/, storage.py, or run.py.
  - GCS paths are defined in config.yaml; read from self.config here.
"""

import logging
from datetime import datetime, timezone

from base.scraper import BaseScraper
from scrapers.cbb.torvik_scraper import scrape_torvik

logger = logging.getLogger(__name__)


class TorvikScraper(BaseScraper):
    """
    Scrapes Bart Torvik team ratings for full-season and last-10-games splits.

    GCS outputs (configured in config.yaml):
      cbb/torvik_team.json   — full-season normalized team list
      cbb/torvik_splits.json — both splits with metadata (no raw HTML)
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
                        Raised before any GCS write; entire run aborts.
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

    def validate(self, data: dict) -> bool:
        """
        Structural completeness check: did we get enough teams to be worth writing?

        Note: Statistical validation (null rates, outlier detection) is the
        responsibility of jfmoon/sports-analysis, not this layer.
        """
        full = data.get("full_season", {})
        team_count = full.get("metadata", {}).get("team_count", 0)
        if team_count < 350:
            logger.error(
                "Torvik validate: full_season team_count=%d < 350 — aborting write.",
                team_count,
            )
            return False
        return True

    def upsert(self, data: dict) -> None:
        """
        Write two GCS snapshots:
          1. cbb/torvik_team.json     — full-season team list only (latest snapshot)
          2. cbb/torvik_splits.json   — normalized_data + metadata for all splits

        Raw HTML is NOT written to the snapshot paths. The archive write
        (timestamped /raw/ path) is handled by ScraperRunner via StorageManager.
        """
        full_season_teams = data["full_season"]["normalized_data"]
        self.storage.write_json(
            self.config.get("gcs_object", "cbb/torvik_team.json"),
            {
                "updated":    datetime.now(timezone.utc).isoformat(),
                "team_count": len(full_season_teams),
                "teams":      full_season_teams,
            },
        )

        # Multi-split snapshot: normalized data + metadata per split, no raw HTML.
        splits_snapshot = {
            split: {
                "data":     payload["normalized_data"],
                "metadata": payload["metadata"],
            }
            for split, payload in data.items()
        }
        self.storage.write_json("cbb/torvik_splits.json", splits_snapshot)

        logger.info(
            "Torvik: wrote %d teams to %s and splits to cbb/torvik_splits.json",
            len(full_season_teams),
            self.config.get("gcs_object", "cbb/torvik_team.json"),
        )
