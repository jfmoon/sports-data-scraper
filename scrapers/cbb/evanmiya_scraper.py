"""
scrapers/cbb/evanmiya_scraper.py

Standalone EvanMiya team ratings scraper.

Architecture
------------
EvanMiya is a Shiny/R app. Team data is rendered directly into the DOM as a
Reactable (.ReactTable / .rt-table) component — NOT via XHR or AG Grid.
Do NOT use XHR interception or AG Grid selectors here.

Auth / session
--------------
Login uses Playwright's storage_state (cookies + localStorage) saved to
data/evanmiya_auth_state.json after the first manual login. Subsequent runs
restore the session automatically without requiring another manual login.

On first run (no state file): opens a visible browser, prompts for manual login,
saves session state, then proceeds. On subsequent runs: restores session and
goes directly to scraping.

Wait strategy
-------------
The Shiny/R Reactable table re-renders multiple times during load, causing
Playwright's wait_for_selector to loop on elements that resolve but keep
moving. Instead we use:
  1. wait_for_load_state("networkidle") to let the Shiny app settle
  2. time.sleep(RENDER_SETTLE_S) as a fixed dwell for final render
  3. query_selector_all() to read the stable DOM

This matches what the DOM inspection script used (time.sleep(dwell)) and
avoids the timeout loop caused by wait_for_selector on .rt-tr-group.

URL
---
Correct team ratings URL: https://evanmiya.com/?ratings=team-ratings
The homepage preview (?team_ratings) only shows 5 columns and 10 rows.

CLI usage (from repo root):
    python -m scrapers.cbb.evanmiya_scraper
    python -m scrapers.cbb.evanmiya_scraper --login   # force fresh login
    python -m scrapers.cbb.evanmiya_scraper --out /tmp/evanmiya.json
"""

import sys
import os
import json
import time
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from scrapers.cbb.names import to_canonical

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEAM_RATINGS_URL = "https://evanmiya.com/?team_ratings"

# Saved session state — reused across runs to avoid repeated manual login
STATE_FILE = Path("data/evanmiya_auth_state.json")

# How long to wait for login confirmation (seconds, used in the countdown)
LOGIN_WAIT_S = 180

# After networkidle, additional dwell for Shiny/Reactable final render (seconds)
RENDER_SETTLE_S = 10

# Polite countdown after login before scraping (seconds) — matches KenPom pattern
POST_LOGIN_DELAY_S = 30

# Minimum acceptable team rows — partial loads must not pass
MIN_TEAM_COUNT = 300

# Reactable DOM selectors
SEL_TABLE  = ".ReactTable"
SEL_ROW    = ".rt-tr-group"
SEL_HEADER = ".rt-th"
SEL_CELL   = ".rt-td"

# Columns that MUST be present for the scrape to be valid
REQUIRED_HEADERS = {"Team", "O-Rate", "D-Rate", "Relative Rating"}

# Visible header label → output field name.
# Header cells have tooltip JSON appended after the visible label; strip from '{'.
HEADER_MAP: dict[str, str] = {
    "Rank":            "rank",
    "Relative Ranking": "rank",
    "Team":            "name",
    "O-Rate":          "o_rate",
    "D-Rate":          "d_rate",
    "Net Rate":        "relative_rating",
    "Relative Rating": "relative_rating",
    "Opp Adjust":      "opp_adjust",
    "Opponent Adjust": "opp_adjust",
    "Roster Rank":     "roster_rank",
    "Pace Adjust":     "pace_adjust",
    "Off Rank":        "off_rank",
    "Def Rank":        "def_rank",
    "True Tempo":      "true_tempo",
    "Tempo Rank":      "tempo_rank",
    "Injury Rank":     "injury_rank",
    "Home Rank":       "home_rank",
}

FLOAT_FIELDS = {"o_rate", "d_rate", "relative_rating", "opp_adjust",
                "pace_adjust", "true_tempo"}
INT_FIELDS   = {"rank", "roster_rank", "off_rank", "def_rank",
                "tempo_rank", "injury_rank", "home_rank"}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _clean_header(raw: str) -> str:
    """Strip tooltip JSON blob and whitespace from a Reactable header cell."""
    return raw.split("{")[0].strip()


def _clean_cell(raw: str) -> str:
    """
    Strip tooltip JSON and trailing emoji/noise from a Reactable cell value.

    Team cells may contain appended emoji + tooltip JSON, e.g.:
        'Duke \U0001f3c0{"x":{"opts":...}}'
    We strip from the first '{' then rstrip whitespace and stray emoji.
    """
    text = raw.split("{")[0].strip()
    # Remove any trailing non-ASCII characters (emoji appended before the JSON)
    text = text.rstrip()
    while text and ord(text[-1]) > 127:
        text = text[:-1].rstrip()
    return text


def _to_float(val: str) -> Optional[float]:
    try:
        return float(val.replace(",", "").strip())
    except (ValueError, TypeError, AttributeError):
        return None


def _to_int(val: str) -> Optional[int]:
    try:
        s = val.replace(",", "").strip()
        if "." in s:
            return None  # don't silently truncate "17.3" → 17
        return int(s)
    except (ValueError, TypeError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Table identification
# ---------------------------------------------------------------------------

def _find_team_ratings_table(page):
    """
    Find the Reactable table whose headers include 'Team' and 'O-Rate'/'D-Rate'.
    Returns the first matching Playwright ElementHandle, or None.
    """
    # Scope search strictly to #shiny-tab-team_ratings.
    # Do NOT fall back to full page — the homepage preview table would be found instead.
    pane = page.query_selector("#shiny-tab-team_ratings")
    if pane is None:
        raise ValueError(
            "EvanMiya: #shiny-tab-team_ratings pane not found. "
            "The page may not have loaded or the session may be stale. "
            "Re-run with --login to refresh the session."
        )
    tables = pane.query_selector_all(SEL_TABLE)
    logger.debug("Found %d ReactTable elements in team ratings pane", len(tables))

    for i, table in enumerate(tables):
        header_els = table.query_selector_all(SEL_HEADER)
        cleaned = {_clean_header(h.inner_text()) for h in header_els}
        has_team = "Team" in cleaned
        has_rate = bool(cleaned & {"O-Rate", "D-Rate"})
        logger.debug("Table %d headers: %s", i, sorted(cleaned))
        if has_team and has_rate:
            logger.info("Selected table %d as team ratings table", i)
            return table

    return None


# ---------------------------------------------------------------------------
# Row extraction and normalization
# ---------------------------------------------------------------------------

def _extract_raw_rows(table) -> tuple[list[str], list[dict[str, str]]]:
    """
    Extract headers and raw cell-text rows from the identified Reactable table.
    Returns (cleaned_headers, rows) where rows is a list of header→value dicts.
    Raises ValueError if required columns are missing.
    """
    header_els = table.query_selector_all(SEL_HEADER)
    cleaned_headers = [_clean_header(h.inner_text()) for h in header_els]

    missing = REQUIRED_HEADERS - set(cleaned_headers)
    if missing:
        raise ValueError(
            f"EvanMiya: required columns missing: {missing}. "
            f"Actual headers: {cleaned_headers}. "
            f"Page may not have fully loaded or login is incomplete."
        )

    row_els = table.query_selector_all(SEL_ROW)
    logger.info("Found %d row elements in table", len(row_els))

    rows = []
    for row in row_els:
        cells = row.query_selector_all(SEL_CELL)
        vals  = [_clean_cell(c.inner_text()) for c in cells]
        if len(vals) < len(cleaned_headers):
            logger.debug("Skipping short row (%d cells): %s", len(vals), vals)
            continue
        rows.append(dict(zip(cleaned_headers, vals)))

    return cleaned_headers, rows


def _normalize_row(raw: dict[str, str]) -> Optional[dict]:
    """
    Convert a raw header-keyed row dict to a normalized output record.
    Returns None if no team name is present (row is skipped).
    Raises ValueError if team name cannot be canonicalized (caller accumulates).
    """
    out: dict = {}
    for header, raw_val in raw.items():
        field = HEADER_MAP.get(header)
        if field is None:
            continue

        if field == "name":
            out["name"] = to_canonical(raw_val, source="evanmiya", strict=True)
        elif field in FLOAT_FIELDS:
            out[field] = _to_float(raw_val)
        elif field in INT_FIELDS:
            out[field] = _to_int(raw_val)

    return out if "name" in out else None


# ---------------------------------------------------------------------------
# Login / session management
# ---------------------------------------------------------------------------

def _login_and_save_state(page, context) -> None:
    """
    Prompt the user to log in manually, then save the session state to disk.
    The state file is reused on all subsequent runs.
    """
    print(f"\nEvanMiya: navigating to {TEAM_RATINGS_URL}")
    page.goto(TEAM_RATINGS_URL, wait_until="domcontentloaded", timeout=60_000)
    print("\nLog in to EvanMiya in the browser, then press ENTER here.")
    input()
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(STATE_FILE))
    print(f"Session saved to {STATE_FILE} — future runs will skip manual login.\n")


def _wait_for_render(page) -> None:
    """
    Wait for the Shiny app to settle and the team ratings table to appear.

    Waits for the table inside #shiny-tab-team_ratings specifically, with a
    fixed dwell after it appears. Using wait_for_selector on the scoped
    selector avoids the loop issue (which occurred when waiting on any
    .rt-tr-group — the homepage preview tables kept re-resolving).
    """
    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass

    # Wait for the table specifically inside the team ratings pane
    try:
        page.wait_for_selector(
            "#shiny-tab-team_ratings .ReactTable",
            timeout=30_000,
            state="attached",
        )
    except Exception:
        pass  # fall through to dwell — table may still be there

    logger.debug("Settling for %ds after table detected...", RENDER_SETTLE_S)
    time.sleep(RENDER_SETTLE_S)


# ---------------------------------------------------------------------------
# Page size
# ---------------------------------------------------------------------------

def _set_page_size(page, size: int = 500, timeout_s: int = 15) -> None:
    """
    Set the Reactable page size select to show all rows, then poll until
    the row count exceeds the default 50.

    The table defaults to 50 rows per page. Setting to 500 exposes all
    ~365 D-I teams. We poll rather than use a fixed sleep because re-render
    time varies.
    """
    page.evaluate(
        """(size) => {
            const pane = document.getElementById('shiny-tab-team_ratings');
            const sel = pane?.querySelector('.rt-page-size-select');
            if (sel) {
                sel.value = String(size);
                sel.dispatchEvent(new Event('change', { bubbles: true }));
            }
        }""",
        size,
    )
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        row_count = page.evaluate(
            """() => {
                const pane = document.getElementById('shiny-tab-team_ratings');
                return pane?.querySelector('.ReactTable')
                    ?.querySelectorAll('.rt-tr-group').length ?? 0;
            }"""
        )
        if row_count > 50:
            logger.info("EvanMiya: page size set — %d rows now visible", row_count)
            return
        time.sleep(0.5)
    logger.warning(
        "EvanMiya: timed out waiting for >50 rows after setting page size — "
        "proceeding with %d rows", row_count if 'row_count' in dir() else 0
    )


# ---------------------------------------------------------------------------
# Public scrape function
# ---------------------------------------------------------------------------

def scrape_evanmiya(visible: bool = True, force_login: bool = False) -> list[dict]:
    """
    Scrape EvanMiya team ratings from the authenticated Reactable DOM table.

    On first run (no saved session): opens a visible browser and prompts for
    manual login, then saves the session for future runs.

    On subsequent runs: restores the saved session and scrapes without
    requiring manual login.

    Args:
        visible:     Open a visible browser (required for first-run login).
        force_login: Ignore any saved session and re-authenticate manually.

    Returns:
        List of normalized team dicts with at minimum:
            name, rank, o_rate, d_rate, relative_rating
        Plus optional fields if present in the live table:
            opp_adjust, roster_rank, pace_adjust, off_rank, def_rank,
            true_tempo, tempo_rank, injury_rank, home_rank

    Raises:
        ImportError  – playwright not installed
        ValueError   – table not found, required columns missing,
                       any unresolved team name, or row count below threshold
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError(
            "playwright is required. "
            "Install with: pip install playwright && playwright install chromium"
        ) from exc

    need_login = force_login or not STATE_FILE.exists()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not visible))

        if need_login:
            context = browser.new_context()
            page    = context.new_page()
            _login_and_save_state(page, context)
        else:
            context = browser.new_context(storage_state=str(STATE_FILE))
            page    = context.new_page()

        # Navigate to the full team ratings page
        print(f"EvanMiya: navigating to {TEAM_RATINGS_URL}")
        page.goto(TEAM_RATINGS_URL, wait_until="domcontentloaded", timeout=60_000)

        # Polite delay with visible countdown (matches KenPom pattern)
        print(f"Logged in — waiting {POST_LOGIN_DELAY_S} seconds before starting scrape...")
        for i in range(POST_LOGIN_DELAY_S, 0, -5):
            print(f"  Starting in {i}s...")
            time.sleep(5)
        print("Starting scrape...\n")

        # Wait for Shiny render to settle
        _wait_for_render(page)

        # Locate the team ratings table
        table = _find_team_ratings_table(page)
        if table is None:
            try:
                page.screenshot(path="/tmp/evanmiya_no_table.png")
                print("Debug screenshot saved to /tmp/evanmiya_no_table.png")
            except Exception:
                pass
            raise ValueError(
                "EvanMiya: could not find a ReactTable with 'Team' and "
                "'O-Rate'/'D-Rate' headers. "
                "The page may not have rendered fully — try re-running. "
                "If this persists, run with --login to refresh the session."
            )

        _set_page_size(page)
        cleaned_headers, raw_rows = _extract_raw_rows(table)
        logger.info("EvanMiya: extracted %d raw rows", len(raw_rows))

        browser.close()

    # Normalize and canonicalize outside the browser context
    unresolved: list[str] = []
    records:    list[dict] = []
    seen:       set[str]   = set()

    for raw in raw_rows:
        try:
            record = _normalize_row(raw)
        except ValueError as exc:
            unresolved.append(str(exc))
            continue

        if record is None:
            continue

        name = record["name"]
        if name in seen:
            logger.warning("EvanMiya: duplicate canonical name '%s' — skipping", name)
            continue
        seen.add(name)
        records.append(record)

    # Fail loudly on unresolved names — do not write partial data
    if unresolved:
        raise ValueError(
            f"EvanMiya: {len(unresolved)} team name(s) could not be resolved. "
            f"Add aliases to data/crosswalks/cbb_teams.json.\n"
            + "\n".join(f"  {e}" for e in unresolved)
        )

    # Fail loudly on partial loads
    if len(records) < MIN_TEAM_COUNT:
        raise ValueError(
            f"EvanMiya: only {len(records)} teams extracted "
            f"(minimum expected: {MIN_TEAM_COUNT}). "
            f"Table may not have fully rendered — re-run or increase RENDER_SETTLE_S."
        )

    logger.info("EvanMiya: successfully extracted %d teams", len(records))
    return records


# ---------------------------------------------------------------------------
# Content key (used by framework wrapper)
# ---------------------------------------------------------------------------

def build_content_key(records: list[dict]) -> str:
    """
    Deterministic SHA-256 hash over sorted normalized records.
    Only data fields hashed — no timestamps or metadata.
    """
    HASH_FIELDS = (
        "name", "rank", "o_rate", "d_rate", "relative_rating",
        "opp_adjust", "roster_rank", "pace_adjust", "off_rank", "def_rank",
        "true_tempo", "tempo_rank", "injury_rank", "home_rank",
    )
    sorted_records = sorted(records, key=lambda r: r.get("name", ""))
    stable = [
        {k: r.get(k) for k in HASH_FIELDS if k in r}
        for r in sorted_records
    ]
    payload = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="EvanMiya standalone scraper")
    parser.add_argument(
        "--out", default="/tmp/evanmiya_debug.json",
        help="Output path for debug JSON (default: /tmp/evanmiya_debug.json)",
    )
    parser.add_argument(
        "--login", action="store_true",
        help="Force fresh manual login (ignores saved session state)",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run headless (only works with a valid saved session)",
    )
    args = parser.parse_args()

    teams = scrape_evanmiya(visible=not args.headless, force_login=args.login)

    output = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "team_count": len(teams),
        "teams": teams,
    }
    with open(args.out, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {len(teams)} teams to {args.out}")
    if teams:
        print("Sample record:")
        print(json.dumps(teams[0], indent=2))
