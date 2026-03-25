"""
scrapers/nhl/natural_stat_trick_scraper.py

STANDALONE Playwright scraper for Natural Stat Trick.
Must be run as a module from the repo root:
  python -m scrapers.nhl.natural_stat_trick_scraper --season 2025 --out /tmp/nst.json

Called by the framework wrapper (natural_stat_trick.py) via subprocess.run().
Do NOT import this module directly — it is subprocess-invoked only.

Why standalone:
  naturalstattrick.com serves a Cloudflare JS browser verification challenge.
  requests, headless Playwright, and playwright-stealth all fail.
  A visible Chromium browser is required — identical to Torvik's pattern.
  No login required. The challenge clears after the first page load.

Dependencies (not in base requirements.txt — install separately):
  pip install playwright beautifulsoup4 lxml
  playwright install chromium

Output JSON schema:
  {
    "season": 2025,
    "data": {
      "team_5v5":   [... list of row dicts ...],
      "team_all":   [... list of row dicts ...],
      "player_5v5": [... list of row dicts ...]
    },
    "status": {
      "team_5v5":   "success" | "cloudflare_block" | "timeout" | "empty_result" | "error",
      "team_all":   ...,
      "player_5v5": ...
    },
    "warnings": [... list of non-fatal warning strings ...]
  }
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

NST_BASE = "https://www.naturalstattrick.com"


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class TableFetch:
    status: str                              # success | cloudflare_block | timeout | empty_result | error
    rows: list[dict] = field(default_factory=list)
    reason: str | None = None


# ---------------------------------------------------------------------------
# Header normalization
# ---------------------------------------------------------------------------

def normalize_header(h: str) -> str:
    """Standardize NST column headers to snake_case.

    Handles stat symbols (+/-, %, /60) before general punctuation stripping
    to avoid mangling compound expressions.
    """
    h = h.strip().lower()
    h = h.replace("+/-", "plus_minus")
    h = h.replace("%", "_pct")
    h = h.replace("/60", "_per_60")
    h = h.replace("/", "_per_")
    h = h.replace("-", "_")
    h = h.replace(" ", "_")
    h = re.sub(r'[^a-z0-9_]', '', h)
    h = re.sub(r'_+', '_', h)
    return h.strip("_")


# ---------------------------------------------------------------------------
# HTML table parser
# ---------------------------------------------------------------------------

def parse_nst_table(html: str, label: str, required_keys: list[str]) -> list[dict]:
    """Parse an NST HTML DataTable into a list of row dicts.

    Selection priority:
      1. <table id="teams"> or <table id="players">
      2. First <table class="dataTable">

    Multi-row headers: uses only the last <tr> in <thead>.
    Colspan: expands into indexed column names (e.g. "cf_1", "cf_2").

    Identity guard: rows missing required_keys (None or empty string) are
    logged and dropped. 0 and 0.0 are valid values and are kept.

    Numeric conversion: strips commas and % signs, then tries int then float.
    Non-numeric values are kept as strings.
    """
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", id=re.compile(r"teams|players"))
    if not table:
        table = soup.select_one("table.dataTable")

    if not table or not table.find("thead") or not table.find("tbody"):
        logger.warning("[%s] No valid table structure found.", label)
        return []

    # Use last header row to handle multi-level column groups
    header_rows = table.find("thead").find_all("tr")
    target_row = header_rows[-1]

    headers: list[str] = []
    for th in target_row.find_all("th"):
        name = normalize_header(th.get_text()) or "col"
        colspan = int(th.get("colspan", 1))
        for i in range(colspan):
            suffix = f"_{i + 1}" if colspan > 1 else ""
            headers.append(f"{name}{suffix}")

    rows: list[dict] = []
    for tr in table.find("tbody").find_all("tr"):
        if "No data available" in tr.get_text():
            continue

        cells = tr.find_all("td")
        if not cells:
            continue

        if len(cells) > len(headers):
            logger.warning(
                "[%s] Row has %d cells vs %d headers — trailing cells truncated.",
                label, len(cells), len(headers),
            )

        row_data: dict = {}
        for i, cell in enumerate(cells):
            if i >= len(headers):
                break
            key = headers[i]
            val = cell.get_text(strip=True)
            clean = val.replace(",", "").replace("%", "")
            try:
                row_data[key] = float(clean) if "." in clean else int(clean)
            except ValueError:
                row_data[key] = val

        # Identity guard — required fields must be present and non-empty
        missing = [k for k in required_keys if row_data.get(k) in (None, "")]
        if missing:
            logger.error(
                "[%s] Identity guard failed — missing keys %s in row: %s",
                label, missing, row_data,
            )
            continue

        rows.append(row_data)

    return rows


# ---------------------------------------------------------------------------
# Per-section fetch
# ---------------------------------------------------------------------------

def fetch_section(
    page: Page, url: str, label: str, required_keys: list[str]
) -> TableFetch:
    """Navigate to one NST report URL and extract its data table.

    NST serves a Cloudflare checkbox challenge on each report URL independently —
    session cookies do not carry over between pages. The scraper navigates to the
    URL, then pauses and waits for the user to press Enter after clearing the
    challenge (or immediately if no challenge appears).
    """
    logger.info("[%s] Navigating to: %s", label, url)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(1_500)

        print()
        print("=" * 60)
        print(f"NST [{label}]: Page loaded in browser.")
        print("If a Cloudflare checkbox is visible, click it now.")
        print("Press Enter when the data table is visible...")
        print("=" * 60)
        input()

        rows = parse_nst_table(page.content(), label, required_keys)

        lower = page.content().lower()
        if any(x in lower for x in ["cf-challenge", "checking your browser", "verify you are human"]):
            logger.error("[%s] Cloudflare challenge still active — table not loaded.", label)
            return TableFetch(status="cloudflare_block")

        if not rows:
            return TableFetch(status="empty_result")

        logger.info("[%s] Parsed %d rows.", label, len(rows))
        return TableFetch(status="success", rows=rows)

    except Exception as exc:
        logger.exception("[%s] Unhandled exception during fetch.", label)
        return TableFetch(status="error", reason=str(exc))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Standalone NST Playwright scraper — run as module from repo root"
    )
    parser.add_argument(
        "--season", type=int, required=True,
        help="First year of the season, e.g. 2025 for 2025-26",
    )
    parser.add_argument(
        "--out", type=str, required=True,
        help="Output JSON file path",
    )
    parser.add_argument(
        "--headless", action="store_true", default=False,
        help="Run headless (not recommended — NST requires visible browser for Cloudflare)",
    )
    args = parser.parse_args()

    s_str = f"{args.season}{args.season + 1}"
    base_params = {
        "fromseason": s_str,
        "thruseason": s_str,
        "stype": "2",       # regular season
        "score": "all",
        "loc": "B",
        "gpfilt": "none",
    }

    # Report endpoints and their identity-guard required keys
    configs: dict[str, dict] = {
        "team_5v5": {
            # rate=n returns raw totals including toi column; rate=y returns per-60 rates
            # but replaces toi with toi_per_gp. Use rate=n so toi identity guard works.
            "url": f"{NST_BASE}/teamtable.php?{urlencode({**base_params, 'sit': '5v5', 'rate': 'n', 'team': 'all', 'toi': '0'})}",
            "req": ["team", "gp"],
        },
        "team_all": {
            "url": f"{NST_BASE}/teamtable.php?{urlencode({**base_params, 'sit': 'all', 'rate': 'n', 'team': 'all', 'toi': '0'})}",
            "req": ["team", "gp"],
        },
        "player_5v5": {
            "url": f"{NST_BASE}/playerteams.php?{urlencode({**base_params, 'sit': '5v5', 'stdoi': 'oi', 'rate': 'r', 'team': 'ALL', 'pos': 'S', 'toi': '50', 'lines': 'single'})}",
            "req": ["player", "team", "gp", "toi"],
        },
    }

    payload: dict = {
        "season": args.season,
        "data": {},
        "status": {},
        "warnings": [],
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            # Visit landing page. Each subsequent report URL may also trigger
            # a Cloudflare challenge — the scraper will pause and prompt you
            # to press Enter after clearing each one.
            logger.info("Opening NST in browser...")
            page.goto(NST_BASE, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1_500)
            print()
            print("=" * 60)
            print("NST: Landing page loaded.")
            print("If a Cloudflare checkbox appears, click it.")
            print("Press Enter to continue to the report pages...")
            print("=" * 60)
            input()

            for label, conf in configs.items():
                result = fetch_section(page, conf["url"], label, conf["req"])
                payload["status"][label] = result.status
                if result.status == "success":
                    payload["data"][label] = result.rows
                else:
                    msg = f"{label} failed: {result.status}"
                    if result.reason:
                        msg += f" ({result.reason})"
                    payload["warnings"].append(msg)
                    logger.warning(msg)

        finally:
            browser.close()

    out_path = Path(args.out)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "NST scraper finished. %d sections succeeded. Output: %s",
        sum(1 for s in payload["status"].values() if s == "success"),
        args.out,
    )


if __name__ == "__main__":
    main()
