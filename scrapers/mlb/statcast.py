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
