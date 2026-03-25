# Runbook — Browser-Based and Long-Running Scrapers

Operational procedures for scrapers that require special handling: visible browsers,
manual login, long runtimes, or known environmental constraints.

---

## KenPom

**Browser mode:** Visible Chromium (not headless)  
**Login required:** Yes — manual  
**Cloud Run:** ❌ Local only

### How to run

```bash
# Full ratings scrape (top 150 teams, writes to cbb/kenpom.json)
python run.py --source kenpom

# FanMatch predictions (today's games)
python run.py --source kenpom --fanmatch

# Standalone — custom top N or specific output path
python scrapers/cbb/kenpom_scraper.py --top 68 --out kenpom.csv
python scrapers/cbb/kenpom_scraper.py --fanmatch
```

### What happens when you run it

1. A visible Chromium browser opens and navigates to kenpom.com
2. **You must log in manually.** Handle any CAPTCHA if prompted.
3. The script waits up to 3 minutes for `#ratings-table` to appear.
4. Once the table is detected, the script waits an additional 30 seconds (polite
   delay) then proceeds automatically.
5. Data is scraped, written to a local CSV, and then uploaded to GCS.

### Known failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| Script exits immediately after browser opens | Login not completed within 3-minute timeout | Re-run and log in faster |
| `#ratings-table` not found | KenPom changed their page structure | Inspect live HTML, update selector in `kenpom_scraper.py` |
| GCS write succeeds but `team_count: 0` | Parse failed silently | Check `data/raw/kenpom/` for the raw CSV; open it manually to verify content |
| CAPTCHA loop | KenPom anti-bot triggered | Wait 10–15 minutes, try again from a different IP if repeated |

### Verify GCS write

```bash
gsutil cat gs://sports-data-scraper-491116/cbb/kenpom.json | python -m json.tool | head -20
# Expect: schema_version, generated_at, team_count >= 68, teams array with adj_o/adj_d values
```

---

## Torvik

**Browser mode:** Visible Chromium (not headless)  
**Login required:** No  
**Cloud Run:** ❌ Local only

### How to run

```bash
# Via framework (writes to cbb/torvik_team.json)
python run.py --source torvik

# Standalone — MUST use -m module form from repo root
python -m scrapers.cbb.torvik_scraper --split full_season
python -m scrapers.cbb.torvik_scraper --split last_10
```

**Critical:** Running `python scrapers/cbb/torvik_scraper.py` directly (without
`-m`) will fail with an import error. The `-m` module form is required.

### What happens when you run it

1. A visible Chromium browser opens and navigates to barttorvik.com.
2. The page serves a Cloudflare JS verification challenge. The visible browser
   passes this automatically — no interaction required.
3. The page renders, data is extracted, team names are canonicalized via
   `scrapers/cbb/names.py` with `strict=True`.
4. Any unresolved team name raises `ValueError` immediately — the scraper halts
   before writing anything to GCS.

### Known failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `ValueError: unresolved team name` | New team name variant not in `cbb_teams.json` | Add the alias to `data/crosswalks/cbb_teams.json`; re-run |
| `RuntimeError: only N teams — expected >= 350` | Partial page load or site outage | Re-run; if persistent, check barttorvik.com manually |
| Browser opens but page never loads | Cloudflare behavior changed | Check if `requests`-based fetch now works; if not, investigate new challenge type |
| `python scrapers/cbb/torvik_scraper.py` gives ImportError | Wrong invocation form | Use `python -m scrapers.cbb.torvik_scraper` from repo root |

### Verify GCS write

```bash
gsutil cat gs://sports-data-scraper-491116/cbb/torvik_team.json | python -m json.tool | head -10
# Expect: schema_version, team_count: 365, teams array with adj_o/adj_d/t_rank values
```

---

## TennisAbstract

**Browser mode:** Headless Chromium + `playwright-stealth`  
**Login required:** No  
**Cloud Run:** ✅ (runs headless — but runtime is ~60–80 min for 250 players)

### How to run

```bash
# Full ranked scrape — top 250 WTA players (writes to tennis/players.json)
python run.py --source tennisabstract

# Single player test
python scrapers/tennis/tennisabstract_scraper.py IgaSwiatek

# Top N
python scrapers/tennis/tennisabstract_scraper.py --top 100
```

### What happens when you run it

1. For each player, a **fresh** `sync_playwright()` instance is created, a new
   browser is launched, a new stealth context is opened, and the player page is
   scraped.
2. After scraping, the browser is closed completely before moving to the next player.
3. This fresh-browser-per-player pattern is mandatory. Cloudflare tracks session
   state at the browser context level — reusing any context causes blocks after
   the first successful load.
4. Runtime: ~15–20 seconds per player. For 250 players: ~60–80 minutes total.
5. Priority players (defined in `config.yaml`) that fall outside the top N are
   scraped individually after the ranked list. Failures on priority players are
   hard errors; failures on optional players log a WARNING and continue.

### Known failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| Consistent blocks after first player | Browser context being reused | Verify `tennisabstract_scraper.py` creates a new `sync_playwright()` per player |
| `curl_cffi` import error | `curl_cffi` was removed from this scraper | Do not add it back — it is consistently blocked on this site |
| `player_count: 0` in output | Ranked fetch failed silently | Check `data/raw/tennisabstract/` for the raw merged JSON; look for `errors` array |
| Individual player missing from output | Page structure changed for that player | Run standalone for that player slug and inspect the HTML manually |

### Verify GCS write

```bash
gsutil cat gs://sports-data-scraper-491116/tennis/players.json | python -m json.tool | head -15
# Expect: schema_version, player_count ~250, players array with raw_stats dict
```

---

## Daily Faceoff

**Browser mode:** None — standard `requests` (no Playwright)  
**Login required:** No  
**Cloud Run:** ✅ (but requires timeout ≥ 180s)

### How to run

```bash
# Full run — goalies + line combinations (~60s)
python run.py --source daily_faceoff

# Fast run — goalies only (~5s), skip line combinations
# Set fetch_lines: false in config.yaml under daily_faceoff:, then:
python run.py --source daily_faceoff
```

### What happens when you run it

1. Fetches the starting goalies page (`/starting-goalies/`) — 1 HTTP request.
2. If `fetch_lines: true` (default), fetches the line-combinations page for all
   32 teams — 32 serial HTTP requests with a 1.5-second delay between each.
3. Total runtime with lines: ~60 seconds. Set Cloud Run / Cloud Function timeout
   to **≥ 180 seconds** for this scraper.
4. HTML is parsed using BeautifulSoup. DFO is a Next.js/Tailwind app with no
   semantic CSS class names — the parser uses structural anchors:
   - Goalie matchup headers: `span.text-3xl`
   - Goalie columns: `div.w-1/2`
   - Line section: `section#line_combos`
   - Forwards header: `span#forwards`
   - Defense header: `span#defense`
   - Player names: `img[alt]` attributes

### Known failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| `goalie_count: 0` in output | DFO page redesign broke `span.text-3xl` selector | Run scraper locally, inspect live HTML with `soup.select("span.text-3xl")`, update `_parse_goalies()` in `daily_faceoff.py` |
| `entry_count: 0` in `nhl/lines.json` | `section#line_combos` or `span#forwards` not found | Run locally, inspect section structure, update `_parse_lines_for_team()` |
| Cloud Run timeout | Lines fetch running long | Set timeout ≥ 180s in Cloud Run config, or set `fetch_lines: false` for goalie-only runs |
| Fallback parser triggered | `span.text-3xl` not found on goalie page | `starter_status_raw` values will be prefixed with `fallback_parse:` — investigate live HTML |

### Verify GCS write

```bash
gsutil cat gs://sports-data-scraper-491116/nhl/goalies.json | python -m json.tool | head -20
# Expect: schema_version, goalie_count > 0, starter_status values are confirmed/expected/projected/unknown

gsutil cat gs://sports-data-scraper-491116/nhl/lines.json | python -m json.tool | head -5
# Expect: schema_version, entry_count ~250+
```

---

## Natural Stat Trick

**Browser mode:** Needs visible Chromium — **not yet implemented**  
**Login required:** No  
**Cloud Run:** ❌ Not yet (needs Playwright standalone)  
**Current status:** Returns 0 records with WARNING. This is correct behavior.

### Current behavior

`scrapers/nhl/natural_stat_trick.py` exists as a framework wrapper stub. When run,
it immediately returns an empty fetch result with this warning:

```
NST: naturalstattrick.com requires a visible browser (Cloudflare JS challenge).
Returning empty fetch result. Implement natural_stat_trick_scraper.py to enable.
MoneyPuck (nhl/team_stats.json, nhl/player_stats.json) covers equivalent metrics.
```

The `nhl/team_splits.json` and `nhl/player_splits.json` GCS paths are reserved but
not currently written. `nhl/nst_lines.json` is reserved for future NST line data.

### Why it is blocked

naturalstattrick.com serves a Cloudflare JS challenge (`window.__CF$cv$params`)
that is identical in behavior to barttorvik.com. The challenge blocks:

- `requests` — returns the challenge page, not the data table
- Headless Playwright — same
- `playwright-stealth` — same

Only a visible Chromium browser with real user-agent headers passes the challenge.

### How to implement when ready

Follow `torvik_scraper.py` exactly:

1. Create `scrapers/nhl/natural_stat_trick_scraper.py`
2. Use visible Chromium (`playwright.chromium.launch(headless=False)`)
3. No login required — the page loads automatically once the challenge passes
4. Parse the team and player stats tables
5. Update `scrapers/nhl/natural_stat_trick.py` to call the standalone

### Verify current behavior

```bash
python run.py --source natural_stat_trick --force
# Expected: WARNING about Cloudflare, then "Success: natural_stat_trick — 0 records."
# This is correct — not an error.
```
