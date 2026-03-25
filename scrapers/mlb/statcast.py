# RETIRED: 2025-11-01
# Reason: parse() returned a grouped dict, violating the BaseScraper list[dict] contract.
# Replaced by: scrapers/mlb/statcast_pitchers.py and scrapers/mlb/statcast_hitters.py
# Safe to move to _deprecated/: 2026-06-01

"""
scrapers/mlb/statcast.py — RETIRED

This file has been split into:
  - scrapers/mlb/statcast_pitchers.py  ->  mlb/statcast_pitchers.json
  - scrapers/mlb/statcast_hitters.py   ->  mlb/statcast_hitters.json

Reason: The original single-scraper implementation returned dict from parse()
and validate() instead of list[dict], violating the BaseScraper/ScraperRunner
contract. ScraperRunner crashes when attempting to iterate the validation output.

Registry entries: mlb_statcast_pitchers, mlb_statcast_hitters
Config objects:   mlb/statcast_pitchers.json, mlb/statcast_hitters.json
"""

raise ImportError(
    "scrapers/mlb/statcast.py has been retired. "
    "Use statcast_pitchers.py and statcast_hitters.py instead."
)
