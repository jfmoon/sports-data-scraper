"""
KenPom Scraper
--------------
Scrapes the following for the top-N teams by KenPom ranking:

  From the main ratings page:
    - KenPom_Rank, Team, Conference
    - AdjO_Rank, AdjO, AdjD_Rank, AdjD, AdjT_Rank, AdjT

  From each team's individual stats page:
    - 3P_Pct (offensive 3P%)
    - 3PAr (three point attempt rate)
    - FTR (free throw rate)
    - TO_Pct (turnover rate)
    - ORB_Pct (offensive rebound rate)
    - Block_Pct (defensive block%)
    - Steal_Pct
    - Opp_3P_Pct
    - Experience

Usage:
  1. Run:
       python3 kenpom_scraper.py

  Optional flags:
       --top N              Number of teams to scrape (default: 68)
       --out FILE           Output CSV filename (default: kenpom_data.csv)
       --year YYYY          Season year (default: current season)
       --headless           Run browser headlessly (default: visible)
       --debug              Save first team page HTML to duke_debug.html and exit

  FanMatch mode (scrape today's predictions instead of team ratings):
       --fanmatch                        Scrape FanMatch predictions
       --fanmatch-date YYYY-MM-DD        Specific date (default: today)
       --fanmatch-out FILE               Output JSON (default: fanmatch_data.json)

  Examples:
       python3 kenpom_scraper.py --top 100 --out kenpom_2026.csv
       python3 kenpom_scraper.py --debug              # inspect page structure
       python3 kenpom_scraper.py --fanmatch           # today's predictions
       python3 kenpom_scraper.py --fanmatch --fanmatch-date 2026-03-22
       python3 kenpom_scraper.py --fanmatch --fanmatch-out r32_sat.json --fanmatch-date 2026-03-21
"""

import json
import os
import re
import time
import random
import argparse
import csv
import sys
from datetime import datetime, timedelta, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

BASE_URL = "https://kenpom.com"

DELAY_MIN = 4.0
DELAY_MAX = 9.0
LONG_PAUSE_EVERY = 10
LONG_PAUSE_MIN = 15.0
LONG_PAUSE_MAX = 30.0
MAX_RETRIES = 3
RETRY_DELAY = 20.0

# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="KenPom scraper")
    parser.add_argument("--top", type=int, default=68)
    parser.add_argument("--out", type=str, default="kenpom_data.csv")
    parser.add_argument("--year", type=str, default=None)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--debug", action="store_true",
                        help="Save first team page HTML to debug.html and exit")
    parser.add_argument("--retry-failed", type=str, default=None, metavar="CSV",
                        help="Re-scrape teams with missing stats in an existing CSV and update it in-place")
    parser.add_argument("--fanmatch", action="store_true",
                        help="Scrape FanMatch predictions instead of team ratings")
    parser.add_argument("--fanmatch-date", type=str, default=None, metavar="YYYY-MM-DD",
                        help="Specific date for FanMatch (default: today's page)")
    parser.add_argument("--fanmatch-out", type=str, default="fanmatch_data.json",
                        help="Output JSON file for FanMatch data (default: fanmatch_data.json)")
    return parser.parse_args()

# ── Helpers ───────────────────────────────────────────────────────────────────

def polite_delay(step_num: int):
    if step_num > 0 and step_num % LONG_PAUSE_EVERY == 0:
        pause = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
        print(f"  [pause] Longer break ({pause:.1f}s) after {step_num} teams...")
        time.sleep(pause)
    else:
        pause = random.uniform(DELAY_MIN, DELAY_MAX)
        time.sleep(pause)

def safe_text(el) -> str:
    try:
        return el.inner_text().strip()
    except Exception:
        return ""

def parse_rank_value(cell_text: str):
    # KenPom format: "128.0 4" = value first, rank second
    parts = cell_text.split()
    if len(parts) == 2:
        return parts[1], parts[0]  # (rank, value)
    elif len(parts) == 1:
        return "", parts[0]
    return "", cell_text

# ── Login (manual) ────────────────────────────────────────────────────────────

def login(page):
    print("Opening KenPom in browser...")
    print("─" * 50)
    print("ACTION REQUIRED:")
    print("  1. Complete any CAPTCHA if prompted")
    print("  2. Log in with your KenPom credentials")
    print("  3. Wait until you can see the main ratings table")
    print("  Script continues automatically once you're logged in.")
    print("─" * 50)

    page.goto(f"{BASE_URL}/index.php", wait_until="domcontentloaded")

    try:
        page.wait_for_selector("#ratings-table", timeout=180000)
    except PlaywrightTimeout:
        sys.exit("\nERROR: Timed out waiting for login (3 min). Please re-run.")

    print("Logged in — waiting 30 seconds before starting scrape...")
    for i in range(30, 0, -5):
        print(f"  Starting in {i}s...")
        time.sleep(5)
    print("Starting scrape...\n")

# ── Main ratings page ─────────────────────────────────────────────────────────

def scrape_main_ratings(page, top_n: int, year: str = None) -> list:
    url = f"{BASE_URL}/index.php"
    if year:
        url += f"?y={year}"

    print(f"Loading main ratings page: {url}")
    page.goto(url, wait_until="networkidle")
    page.wait_for_selector("#ratings-table", timeout=15000)

    rows = page.query_selector_all("#ratings-table tbody tr")
    teams = []

    for row in rows:
        cells = row.query_selector_all("td")
        if len(cells) < 6:
            continue
        try:
            rank_raw = safe_text(cells[0])
            rank = int(rank_raw) if rank_raw.isdigit() else None
            if rank is None:
                continue
            if rank > top_n:
                break

            team_link = cells[1].query_selector("a")
            team_name_raw = safe_text(team_link) if team_link else safe_text(cells[1])
            # Strip trailing rank number that KenPom appends as superscript e.g. "Duke 1"
            team_name = re.sub(r'\s+\d+$', '', team_name_raw).strip()
            team_href = team_link.get_attribute("href") if team_link else ""
            slug_match = re.search(r'team=([^&]+)', team_href or "")
            team_slug = slug_match.group(1) if slug_match else team_name

            conf = safe_text(cells[2])

            # Each stat occupies two cells: value then rank
            # 0:Rank 1:Team 2:Conf 3:W-L 4:AdjEM 5:AdjO 6:AdjO_Rank 7:AdjD 8:AdjD_Rank 9:AdjT 10:AdjT_Rank
            adjo_val  = safe_text(cells[5])
            adjo_rank = safe_text(cells[6])
            adjd_val  = safe_text(cells[7])
            adjd_rank = safe_text(cells[8])
            adjt_val  = safe_text(cells[9])
            adjt_rank = safe_text(cells[10])

            teams.append({
                "KenPom_Rank": rank,
                "Team": team_name,
                "Team_Slug": team_slug,
                "Conference": conf,
                "AdjO_Rank": adjo_rank,
                "AdjO": adjo_val,
                "AdjD_Rank": adjd_rank,
                "AdjD": adjd_val,
                "AdjT_Rank": adjt_rank,
                "AdjT": adjt_val,
            })

        except Exception as e:
            print(f"  [warn] Skipping row: {e}")
            continue

    print(f"Parsed {len(teams)} teams.\n")
    return teams

# ── Individual team page ──────────────────────────────────────────────────────

def scrape_team_page(page, team_slug: str, year: str = None, retries: int = 0) -> dict:
    url = f"{BASE_URL}/team.php?team={team_slug.replace(' ', '%20')}"
    if year:
        url += f"&y={year}"

    try:
        page.goto(url, wait_until="networkidle", timeout=20000)
    except PlaywrightTimeout:
        if retries < MAX_RETRIES:
            print(f"  [retry] Timeout on {team_slug}, waiting {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
            return scrape_team_page(page, team_slug, year, retries + 1)
        else:
            print(f"  [error] Gave up on {team_slug}.")
            return {}

    stats = {}

    # ── report-table: Category | Offense | Defense | D-I Avg ─────────────────
    # Each row is a stat category. Columns are: [Category, Offense, Defense, D-I Avg]
    # Values contain rank + number packed together e.g. "56.7 12" (value first, rank second)
    # We extract offense value from col[1], defense value from col[2]
    try:
        report_rows = page.evaluate("""
            () => {
                const table = document.querySelector('#report-table');
                if (!table) return null;
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                return rows.map(row => {
                    const cells = Array.from(row.querySelectorAll('td'));
                    return cells.map(c => c.innerText.trim());
                });
            }
        """)

        if report_rows:
            def get_row(keyword):
                """Find a row whose Category cell contains keyword (case-insensitive)."""
                for row in report_rows:
                    if row and keyword.lower() in row[0].lower():
                        return row
                return None

            def offense_val(row):
                """Extract just the numeric value from an Offense cell (drops rank suffix)."""
                if not row or len(row) < 2:
                    return ""
                # Cell format is "56.7 12" — first token is the value
                parts = row[1].split()
                return parts[0] if parts else ""

            def defense_val(row):
                """Extract just the numeric value from a Defense cell."""
                if not row or len(row) < 3:
                    return ""
                parts = row[2].split()
                return parts[0] if parts else ""

            # Shooting
            # Exact category labels from KenPom report-table
            row_3p      = get_row("3P%:")
            row_3par    = get_row("3PA/FGA:")
            row_ftr     = get_row("FTA/FGA:")
            row_to      = get_row("Turnover %:")
            row_orb     = get_row("Off. Reb. %:")
            row_blk     = get_row("Block%:")
            row_stl     = get_row("Steal%:")
            row_exp     = get_row("D-1 Experience:")
            row_ht      = get_row("Average Height:")

            stats["3P_Pct"]     = offense_val(row_3p)
            stats["3PAr"]       = offense_val(row_3par)
            stats["FTR"]        = offense_val(row_ftr)
            stats["TO_Pct"]     = offense_val(row_to)
            stats["ORB_Pct"]    = offense_val(row_orb)
            stats["Block_Pct"]  = defense_val(row_blk)
            stats["Steal_Pct"]  = defense_val(row_stl)
            stats["Opp_3P_Pct"] = defense_val(row_3p)   # opponent 3P% is defense col of same row
            stats["Experience"] = offense_val(row_exp)
            # Average height in inches (e.g. "79.3" from '79.3" 2')
            # Strips the inch symbol and rank before storing
            raw_ht = offense_val(row_ht)
            stats["Avg_Height"] = raw_ht.replace('"', '').strip() if raw_ht else ""

            # Warn if we got mostly empty results
            filled = sum(1 for v in stats.values() if v)
            if filled < 3:
                print(f"  [warn] Low data yield for {team_slug} ({filled} fields populated)")
        else:
            print(f"  [warn] report-table not found for {team_slug}")

    except Exception as e:
        print(f"  [warn] Parse error for {team_slug}: {e}")

    return stats


# ── FanMatch scraper ──────────────────────────────────────────────────────────

# KenPom abbreviates some team names differently from the app.
# Translate FanMatch names → app names before returning.
# Confirmed against live FanMatch pages 2026-03-19 through 2026-03-22.
KENPOM_NAME_MAP = {
    "Connecticut":   "UConn",          # KP uses full name, app uses UConn
    "Iowa St.":      "Iowa State",     # KP abbreviates, app uses full
    "Michigan St.":  "Michigan State", # KP abbreviates, app uses full
    "Utah St.":      "Utah State",     # KP abbreviates, app uses full
    "Miami FL":      "Miami (FL)",     # KP drops parens, app keeps them
    # Add future round names here as needed
}

def _translate_name(name: str) -> str:
    """Translate a KenPom team name to the app's canonical name."""
    return KENPOM_NAME_MAP.get(name, name)


def scrape_fanmatch(page, date_str: str = None) -> list:
    """
    Scrape KenPom FanMatch predictions page.
    Returns list of game dicts (NCAA games only).

    date_str: "YYYY-MM-DD" or None for today's games.
    Team names are translated via KENPOM_NAME_MAP to match app canonical names.

    Game dict fields:
        game, team1, team2, rank1, rank2,
        fav, dog,
        kp_winner, kp_score, kp_win_score, kp_lose_score,
        kp_pct, kp_tempo,
        tip_time, network, location, thrill_score, is_ncaa
    """
    url = f"{BASE_URL}/fanmatch.php"
    if date_str:
        url += f"?d={date_str}"

    print(f"Loading FanMatch: {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_selector("#fanmatch-table", timeout=15000)

    # Extract all rows via JS — same page.evaluate() pattern as existing scraper
    raw_rows = page.evaluate("""
        () => {
            const table = document.getElementById('fanmatch-table');
            if (!table) return [];
            return Array.from(table.querySelectorAll('tbody tr')).map(tr => ({
                cells: Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim())
            }));
        }
    """)

    # Grab the page date heading (e.g. "for Saturday, March 21st (10 games)")
    # Located in div.lh12 as a text node (confirmed via DOM inspection 2026-03-21)
    page_heading = page.evaluate("""
        () => {
            const el = document.querySelector('div.lh12');
            if (el) return el.innerText.trim();
            // Fallback: scan text nodes for the date line
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            let node;
            while ((node = walker.nextNode())) {
                const t = node.textContent.trim();
                if (t.startsWith('for ') && t.includes('games')) return t;
            }
            return document.title;
        }
    """)

    print(f"  Page: {page_heading}")
    print(f"  Rows found: {len(raw_rows)}")

    games = []
    for item in raw_rows:
        cells = item["cells"]
        parsed = _parse_fanmatch_row(cells)
        if parsed:
            games.append(parsed)

    ncaa = [g for g in games if g["is_ncaa"]]
    print(f"  Parsed {len(games)} total, {len(ncaa)} NCAA tournament games")

    # Translate team names to app canonical names
    for g in ncaa:
        for field in ("team1", "team2", "fav", "dog", "kp_winner"):
            if g[field]:
                translated = _translate_name(g[field])
                if translated != g[field]:
                    print(f"  [translate] '{g[field]}' → '{translated}'")
                g[field] = translated

    return ncaa


def _parse_fanmatch_row(cells):
    """
    Parse one FanMatch table row.

    Cell layout (confirmed via DOM inspection 2026-03-21):
      0: Game       e.g. "10 Vanderbilt vs.\n13 Nebraska NCAA"
      1: Prediction e.g. "Vanderbilt 75-74 (52%) [68]"
      2: Time (ET)  e.g. "8:45 pm\n TNT"
      3: Location   e.g. "Oklahoma City, OK\nPaycom Center"
      4: ThrillScore (may include rank suffix e.g. "86.6\n1")
    """
    if len(cells) < 2:
        return None

    # Normalize: collapse all internal whitespace/newlines in each cell
    game_text = " ".join(cells[0].split())
    pred_text = " ".join(cells[1].split())
    time_text = " ".join(cells[2].split()) if len(cells) > 2 else ""
    location  = cells[3].replace("\n", ", ").strip() if len(cells) > 3 else ""
    thrill_raw = cells[4].strip() if len(cells) > 4 else ""
    thrill    = thrill_raw.split()[0] if thrill_raw else ""

    is_ncaa = "NCAA" in game_text

    # Parse game string — handles both "vs." and "at" separators
    # e.g. "10 Vanderbilt vs. 13 Nebraska NCAA"
    #      "69 Dayton at 105 UNC Wilmington"
    game_match = re.match(
        r'^(\d+)\s+(.+?)\s+(?:vs\.|at)\s+(\d+)\s+(.+?)(?:\s+(?:NCAA|NIT))?$',
        game_text
    )

    # Parse prediction — handles decimal win% like 99.6%
    # e.g. "Vanderbilt 75-74 (52%) [68]"
    #      "Arizona 89-60 (99.6%) [70]"
    pred_match = re.match(
        r'^(.+?)\s+(\d+)-(\d+)\s+\((\d+(?:\.\d+)?)%\)\s+\[(\d+)\]$',
        pred_text
    )

    if not pred_match:
        return None

    kp_winner     = pred_match.group(1).strip()
    kp_score      = f"{pred_match.group(2)}-{pred_match.group(3)}"
    kp_win_score  = int(pred_match.group(2))
    kp_lose_score = int(pred_match.group(3))
    kp_pct        = float(pred_match.group(4))
    kp_tempo      = int(pred_match.group(5))

    # Parse tip time and network from time cell
    time_match = re.match(r'^(\d+:\d+\s+(?:am|pm))\s*(.*)$', time_text)
    tip_time = time_match.group(1).strip() if time_match else time_text
    network  = time_match.group(2).strip() if time_match else ""

    if game_match:
        rank1 = int(game_match.group(1))
        team1 = game_match.group(2).strip()
        rank2 = int(game_match.group(3))
        team2 = game_match.group(4).strip()
    else:
        rank1, team1, rank2, team2 = None, None, None, None

    # Identify fav/dog: match KP winner name to team1 or team2 via normalized substring
    fav, dog = team1, team2
    if team1 and team2:
        kp_norm = kp_winner.lower().replace(".", "").replace(" ", "")
        t1_norm = team1.lower().replace(".", "").replace(" ", "")
        t2_norm = team2.lower().replace(".", "").replace(" ", "")
        if kp_norm in t2_norm or t2_norm.startswith(kp_norm) or kp_norm.startswith(t2_norm):
            fav, dog = team2, team1

    try:
        thrill_float = float(thrill) if thrill else None
    except ValueError:
        thrill_float = None

    return {
        "game":          game_text,
        "team1":         team1,
        "team2":         team2,
        "rank1":         rank1,
        "rank2":         rank2,
        "fav":           fav,
        "dog":           dog,
        "kp_winner":     kp_winner,
        "kp_score":      kp_score,
        "kp_win_score":  kp_win_score,
        "kp_lose_score": kp_lose_score,
        "kp_pct":        kp_pct,
        "kp_tempo":      kp_tempo,
        "tip_time":      tip_time,
        "network":       network,
        "location":      location,
        "thrill_score":  thrill_float,
        "is_ncaa":       is_ncaa,
    }

# ── Debug mode ────────────────────────────────────────────────────────────────

def debug_team_page(page, team_slug: str, year: str = None):
    """Save raw HTML + table summary to files for inspection."""
    url = f"{BASE_URL}/team.php?team={team_slug.replace(' ', '%20')}"
    if year:
        url += f"&y={year}"

    print(f"DEBUG: Loading {url}")
    page.goto(url, wait_until="networkidle", timeout=20000)

    # Save full HTML
    html = page.content()
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved full page HTML to debug_page.html")

    # Print table summaries to terminal
    tables = page.evaluate("""
        () => Array.from(document.querySelectorAll('table')).map((t, i) => ({
            index: i,
            id: t.id || '(no id)',
            cls: t.className || '(no class)',
            headers: Array.from(t.querySelectorAll('thead th, thead td')).map(h => h.innerText.trim()),
            preview: t.innerText.slice(0, 300).replace(/\\n+/g, ' | ')
        }))
    """)

    print(f"\nFound {len(tables)} tables on the page:\n")
    for t in tables:
        print(f"  [{t['index']}] id='{t['id']}' class='{t['cls']}'")
        print(f"       headers: {t['headers']}")
        print(f"       preview: {t['preview'][:150]}")
        print()

    # Dump every row of report-table so we can see exact category labels
    report_rows = page.evaluate("""
        () => {
            const table = document.querySelector('#report-table');
            if (!table) return [];
            return Array.from(table.querySelectorAll('tbody tr')).map(row => {
                const cells = Array.from(row.querySelectorAll('td'));
                return cells.map(c => c.innerText.trim());
            });
        }
    """)

    print("-" * 60)
    print("report-table rows (Category | Offense | Defense | D-I Avg):")
    print("-" * 60)
    for row in report_rows:
        print(f"  {row}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=args.headless,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        login(page)
        polite_delay(0)

        # ── FanMatch mode: scrape predictions for one date ────────────────────
        if args.fanmatch:
            # FanMatch is a single fast page — no polite delay needed
            date_str = args.fanmatch_date  # None = today's page
            games = scrape_fanmatch(page, date_str)

            # Determine the actual date label for output
            if date_str:
                label = date_str
            else:
                # ET "today" — page uses server time, just use UTC-4 approximation
                et_now = datetime.now(timezone.utc) - timedelta(hours=4)
                label = et_now.strftime("%Y-%m-%d")

            output = {
                "date":       label,
                "scraped_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "game_count": len(games),
                "games":      games,
            }

            out_file = args.fanmatch_out
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2)
            print(f"\nWrote {len(games)} games to {out_file}")

            # Print summary table
            print(f"\n{'Team':30s}  {'KP%':>6}  {'Score':>8}  {'Time':>10}  Network")
            print("-" * 70)
            for g in games:
                print(f"  {g['kp_winner']:28s}  {g['kp_pct']:>5.1f}%  {g['kp_score']:>8}  {g['tip_time']:>10}  {g['network']}")

            browser.close()
            return

        # ── Retry-failed mode: patch missing rows in existing CSV ───────────────
        if args.retry_failed:
            import urllib.parse
            print(f"\nRetry mode — reading {args.retry_failed}...")

            with open(args.retry_failed, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                rows = list(reader)

            stat_fields = ["3P_Pct", "3PAr", "FTR", "TO_Pct", "ORB_Pct",
                           "Block_Pct", "Steal_Pct", "Opp_3P_Pct", "Experience"]

            failed = [r for r in rows if not any(r.get(f) for f in stat_fields)]
            print(f"Found {len(failed)} teams with missing stats: "
                  f"{[r['Team'] for r in failed]}\n")

            # Build slug map from main ratings page
            all_teams = scrape_main_ratings(page, top_n=999, year=args.year)
            slug_map = {t["Team"]: t["Team_Slug"] for t in all_teams}

            for i, row in enumerate(failed):
                name = row["Team"]
                slug = slug_map.get(name)
                if not slug:
                    # Fallback: derive slug from name
                    slug = urllib.parse.quote(name.replace(" ", "+"))
                print(f"[{i+1}/{len(failed)}] Retrying {name} (slug: {slug})")
                stats = scrape_team_page(page, slug, year=args.year)
                if stats:
                    for f in stat_fields:
                        row[f] = stats.get(f, "")
                    print(f"  ✓ Got data for {name}")
                else:
                    print(f"  ✗ Still failed for {name}")
                polite_delay(i + 1)

            # Write patched CSV back in-place
            with open(args.retry_failed, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

            still_missing = [r["Team"] for r in rows if not any(r.get(f) for f in stat_fields)]
            print(f"\nDone. CSV updated.")
            if still_missing:
                print(f"Still missing data for: {still_missing}")
            browser.close()
            return

        # ── Debug mode: inspect one team page and exit ────────────────────────
        if args.debug:
            # First dump ratings table columns (we're already on index page after login)
            print("\n" + "-" * 60)
            print("ratings-table column dump (first team row):")
            print("-" * 60)
            col_data = page.evaluate("""
                () => {
                    const t = document.querySelector('#ratings-table');
                    if (!t) return null;
                    const headers = Array.from(t.querySelectorAll('thead th'))
                                        .map((h, i) => i + ': ' + h.innerText.trim());
                    const firstRow = t.querySelector('tbody tr');
                    const cells = firstRow
                        ? Array.from(firstRow.querySelectorAll('td'))
                              .map((c, i) => i + ': [' + c.innerText.trim() + ']')
                        : [];
                    return { headers, cells };
                }
            """)
            if col_data:
                print("Headers:")
                for h in col_data["headers"]: print(f"  {h}")
                print("First row cells:")
                for c in col_data["cells"]: print(f"  {c}")
            else:
                print("  ratings-table not found on this page")

            # Then dump team page structure
            print("\nDEBUG MODE — inspecting Duke's team page structure...\n")
            debug_team_page(page, "Duke", year=args.year)

            browser.close()
            print("\nDebug complete. Open debug_page.html in your browser to inspect.")
            print("Share the table output above so we can fix the column mapping.")
            return

        # ── Normal scrape ─────────────────────────────────────────────────────
        print(f"KenPom Scraper")
        print(f"  Teams : top {args.top} | Output: {args.out} | Year: {args.year or 'current'}\n")

        teams = scrape_main_ratings(page, top_n=args.top, year=args.year)
        if not teams:
            sys.exit("ERROR: No teams found on main ratings page.")

        all_rows = []
        total = len(teams)

        for i, team in enumerate(teams):
            slug = team["Team_Slug"]
            name = team["Team"]
            rank = team["KenPom_Rank"]
            print(f"[{i+1}/{total}] {rank}. {name}")

            team_stats = scrape_team_page(page, slug, year=args.year)
            combined = {**team, **team_stats}
            combined.pop("Team_Slug", None)
            all_rows.append(combined)

            polite_delay(i + 1)

        browser.close()

    if not all_rows:
        print("No data collected.")
        return

    fieldnames = [
        "KenPom_Rank", "Team", "Conference",
        "AdjO_Rank", "AdjO",
        "AdjD_Rank", "AdjD",
        "AdjT_Rank", "AdjT",
        "3P_Pct", "3PAr", "FTR", "TO_Pct", "ORB_Pct",
        "Block_Pct", "Steal_Pct", "Opp_3P_Pct",
        "Experience", "Avg_Height",
    ]

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone. {len(all_rows)} teams written to {args.out}")

if __name__ == "__main__":
    main()
