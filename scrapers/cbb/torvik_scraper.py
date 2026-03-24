"""
scrapers/cbb/torvik_scraper.py

Standalone Bart Torvik scraper. Uses a visible Playwright browser to bypass
the JS browser verification challenge on barttorvik.com.

Requires:
    pip install playwright beautifulsoup4
    playwright install chromium

CLI usage:
    python scrapers/cbb/torvik_scraper.py --split full_season
    python scrapers/cbb/torvik_scraper.py --split last_10
"""

import sys
import os
import re
import argparse
import json
import logging
from typing import Any, Optional
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from scrapers.cbb.names import to_canonical

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

class NumericParser:
    @staticmethod
    def to_float(val: Any) -> Optional[float]:
        """Coerce to float; return None on any failure (never raises)."""
        try:
            return float(re.sub(r"[^\d.\-]", "", str(val)).strip())
        except (ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def to_int(val: Any) -> Optional[int]:
        """
        Coerce to int; return None on any failure (never raises).
        Rejects float strings like "127.7" — use to_float for those.
        Handles comma-separated integers like "1,000".
        """
        try:
            s = str(val).strip().replace(",", "")
            # Reject if a decimal point is present — this is a float, not an int
            if "." in s:
                return None
            matched = re.match(r"^-?\d+", s)
            return int(matched.group()) if matched else None
        except (ValueError, TypeError, AttributeError):
            return None


# ---------------------------------------------------------------------------
# Column spec
# ---------------------------------------------------------------------------
# Maps actual Torvik header text (as rendered) → output field name.
# Torvik header row is mixed with aggregate stats; we find the real header
# row by looking for the row that contains 'Rk' and 'Team'.

REQUIRED_COLUMNS: dict[str, str] = {
    "Rk":     "rank",
    "Team":   "name",
    "AdjOE":  "adj_o",
    "AdjDE":  "adj_d",
    "Adj T.": "adj_t",
    "Barthag": "t_rank",
    "WAB":    "wab",
}


def _clean_team_name(raw: str) -> str:
    """
    Strip seed info, emoji, and other appended text from the Team cell.
    Example: 'Michigan1 seed, ✅' -> 'Michigan'
    Torvik appends seed/tournament info directly to the team name string.
    """
    # Remove anything from the first digit onwards (seed number, emoji, etc.)
    cleaned = re.split(r"\d", raw)[0].strip()
    # Remove any remaining non-ASCII characters
    cleaned = cleaned.encode("ascii", "ignore").decode().strip()
    return cleaned


# ---------------------------------------------------------------------------
# Core scrape function
# ---------------------------------------------------------------------------

def scrape_torvik(split: str, year: int = 2026) -> dict:
    """
    Scrape Bart Torvik team ratings for a given split using a visible browser.

    Args:
        split: One of "full_season" or "last_10".
        year:  Season year (default: 2026).

    Returns:
        {
            "normalized_data": list of team dicts (canonical names, typed fields),
            "metadata": {split, year, url, timestamp, team_count, duplicate_count},
        }

    Raises:
        ValueError:  Header drift (required columns missing) or unresolved team name.
        ImportError: If playwright is not installed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright is required for Torvik scraping. "
            "Install with: pip install playwright && playwright install chromium"
        ) from e

    url = f"https://barttorvik.com/trank.php?year={year}"
    if split == "last_10":
        url += "&last=10"

    logger.info("Torvik: fetching %s", url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(2000)
        html = page.content()
        browser.close()

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    table = soup.find("table")
    if not table:
        raise ValueError(
            "Torvik: no table found on page. "
            "The JS verification may not have resolved — try increasing wait_for_timeout."
        )

    # -----------------------------------------------------------------------
    # Find the real header row — the thead contains two rows:
    #   row 0: aggregate D-I averages (not headers)
    #   row 1: actual column headers including 'Rk', 'Team', etc.
    # We identify the header row by checking for presence of 'Rk' and 'Team'.
    # -----------------------------------------------------------------------
    all_rows = table.find_all("tr")
    header_row = None
    header_row_idx = None

    for idx, row in enumerate(all_rows):
        cells = row.find_all(["th", "td"])
        texts = [c.get_text(strip=True) for c in cells]
        if "Rk" in texts and "Team" in texts:
            header_row = texts
            header_row_idx = idx
            break

    if header_row is None:
        raise ValueError(
            f"Torvik header drift: could not find a row containing both 'Rk' and 'Team'. "
            f"First row texts: {[c.get_text(strip=True) for c in all_rows[0].find_all(['th','td'])]}"
        )

    # Verify all required columns are present
    missing = [col for col in REQUIRED_COLUMNS if col not in header_row]
    if missing:
        raise ValueError(
            f"Torvik header drift: required columns not found: {missing}. "
            f"Actual headers: {header_row}"
        )

    indices = {field: header_row.index(col) for col, field in REQUIRED_COLUMNS.items()}

    # -----------------------------------------------------------------------
    # Parse data rows — everything after the header row
    # -----------------------------------------------------------------------
    teams: list[dict] = []
    seen: set[str] = set()
    duplicate_count = 0

    for row in all_rows[header_row_idx + 1:]:
        cells = row.find_all("td")
        if len(cells) <= max(indices.values()):
            continue  # skip spacer or malformed rows

        raw_rank = cells[indices["rank"]].get_text(strip=True)
        if not raw_rank or not raw_rank[0].isdigit():
            continue  # skip aggregate/separator rows

        raw_name = cells[indices["name"]].get_text(strip=True)
        cleaned_name = _clean_team_name(raw_name)

        if not cleaned_name:
            logger.warning("Torvik: empty team name after cleaning '%s', skipping.", raw_name)
            continue

        # Raises ValueError on unresolved name when strict=True —
        # halts the scrape before any data is returned to the wrapper.
        canon_name = to_canonical(cleaned_name, source="torvik", strict=True)

        if canon_name in seen:
            duplicate_count += 1
            logger.warning("Torvik: duplicate canonical name '%s' — skipping.", canon_name)
            continue
        seen.add(canon_name)

        teams.append({
            "name":   canon_name,
            "rank":   NumericParser.to_int(cells[indices["rank"]].get_text(strip=True)),
            "adj_o":  NumericParser.to_float(cells[indices["adj_o"]].get_text(strip=True)),
            "adj_d":  NumericParser.to_float(cells[indices["adj_d"]].get_text(strip=True)),
            "adj_t":  NumericParser.to_float(cells[indices["adj_t"]].get_text(strip=True)),
            "t_rank": NumericParser.to_float(cells[indices["t_rank"]].get_text(strip=True)),
            "wab":    NumericParser.to_float(cells[indices["wab"]].get_text(strip=True)),
        })

    logger.info("Torvik: parsed %d teams (%d duplicates skipped)", len(teams), duplicate_count)

    return {
        "normalized_data": teams,
        "metadata": {
            "split":           split,
            "year":            year,
            "url":             url,
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "team_count":      len(teams),
            "duplicate_count": duplicate_count,
        },
    }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Bart Torvik standalone scraper")
    parser.add_argument(
        "--split",
        choices=["full_season", "last_10"],
        default="full_season",
    )
    parser.add_argument("--year", type=int, default=2026)
    args = parser.parse_args()

    result = scrape_torvik(split=args.split, year=args.year)
    print(json.dumps(result, indent=2))
