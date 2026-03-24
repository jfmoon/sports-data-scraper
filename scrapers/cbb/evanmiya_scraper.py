"""
scrapers/cbb/evanmiya_scraper.py

Standalone EvanMiya scraper using Playwright network interception.
The framework wrapper in scrapers/cbb/evanmiya.py calls this via subprocess.

Requires:
    pip install playwright
    playwright install chromium

CLI usage:
    python scrapers/cbb/evanmiya_scraper.py
"""

import sys
import os
import json
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from scrapers.cbb.names import to_canonical

logger = logging.getLogger(__name__)

# Minimum expected team count. 362 D-I teams; allow for partial AG Grid renders.
MIN_TEAM_COUNT = 350


def scrape_evanmiya() -> dict:
    """
    Scrape EvanMiya team and player ratings by intercepting AG Grid XHR responses.

    Strategy:
        EvanMiya renders data via AG Grid backed by XHR endpoints:
          - /get_team_ratings  — team BPR ratings
          - /get_player_ratings — player ratings
        We intercept all matching responses into additive lists to handle
        pagination, retries, or multi-part responses from the AG Grid backend.
        Using a plain dict would silently overwrite earlier pages with later ones.

    Returns:
        {
            "teams":   list of canonicalized team dicts,
            "players": list of raw player dicts (passed through, no canonicalization),
            "metadata": {timestamp, team_count, player_count},
        }

    Raises:
        ValueError: If fewer than MIN_TEAM_COUNT teams are captured after
                    flattening all intercepted responses.
        ValueError: If any team name cannot be resolved (strict=True).
        ImportError: If playwright is not installed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise ImportError(
            "playwright is required for EvanMiya scraping. "
            "Install with: pip install playwright && playwright install chromium"
        ) from e

    # Additive lists — each XHR response appended, not overwritten.
    team_responses: list[list] = []
    player_responses: list[list] = []

    def handle_response(response):
        if "get_team_ratings" in response.url and response.status == 200:
            try:
                payload = response.json()
                if isinstance(payload, list):
                    team_responses.append(payload)
                    logger.debug(
                        "EvanMiya: intercepted get_team_ratings (%d rows)", len(payload)
                    )
            except Exception:
                logger.warning("EvanMiya: failed to parse get_team_ratings response")

        if "get_player_ratings" in response.url and response.status == 200:
            try:
                payload = response.json()
                if isinstance(payload, list):
                    player_responses.append(payload)
                    logger.debug(
                        "EvanMiya: intercepted get_player_ratings (%d rows)", len(payload)
                    )
            except Exception:
                logger.warning("EvanMiya: failed to parse get_player_ratings response")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.on("response", handle_response)

        logger.info("EvanMiya: loading page...")
        page.goto("https://evanmiya.com/", wait_until="networkidle")
        page.wait_for_selector(".ag-body-viewport", timeout=15_000)

        # Scroll to trigger any lazy-loaded rows in the AG Grid viewport.
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Navigate to player ratings tab to fire the player XHR.
        try:
            page.click("text=Player Ratings")
            page.wait_for_timeout(3000)
        except Exception:
            logger.warning("EvanMiya: could not click 'Player Ratings' tab")

        browser.close()

    # Flatten all intercepted pages into a single list.
    all_teams = [row for page in team_responses for row in page]
    all_players = [row for page in player_responses for row in page]

    logger.info(
        "EvanMiya: captured %d team rows across %d response(s)",
        len(all_teams), len(team_responses),
    )
    logger.info(
        "EvanMiya: captured %d player rows across %d response(s)",
        len(all_players), len(player_responses),
    )

    # Completeness gate — raises before any canonicalization or return.
    if len(all_teams) < MIN_TEAM_COUNT:
        raise ValueError(
            f"EvanMiya completeness failure: captured {len(all_teams)} team rows "
            f"(expected >= {MIN_TEAM_COUNT}). "
            f"The page structure may have changed or AG Grid did not fully render."
        )

    # Canonicalize team names — strict=True halts on first unresolved name.
    final_teams = []
    for row in all_teams:
        raw_name = row.get("team_name", "")
        final_teams.append({
            "team_name":  to_canonical(raw_name, source="evanmiya", strict=True),
            "off_rating": row.get("adj_off_bpr"),
            "def_rating": row.get("adj_def_bpr"),
            "bpr":        row.get("bpr"),
            "rank":       row.get("rank"),
        })

    # Players are passed through raw — no canonical name mapping exists for players.
    return {
        "teams": final_teams,
        "players": all_players,
        "metadata": {
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "team_count":   len(final_teams),
            "player_count": len(all_players),
        },
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = scrape_evanmiya()
    print(json.dumps(result, indent=2, default=str))
