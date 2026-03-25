# Changelog

All notable changes to sports-data-scraper are recorded here.

Add entries at merge time, not after. Breaking output shape changes must also
appear in MIGRATIONS.md (not yet created — add when first breaking change occurs).

**Categories:**
- **Added** — new scrapers, new GCS paths, new fields
- **Changed** — non-breaking behavior changes, additive field additions
- **Breaking** — field renames, removals, type changes
- **Deprecated** — scrapers disabled or marked for removal
- **Operational** — Cloudflare changes, login flow changes, source HTML changes, runtime notes
- **Data** — crosswalk updates, new aliases, franchise name changes
- **Infrastructure** — GCS, Pub/Sub, Cloud Run, dependency changes

---

## [Unreleased]

### Changed
- Added standard envelope fields (`schema_version: 1`, `generated_at`, `scraper_key`,
  `record_count`, `warnings: []`) to all active scraper payloads. All existing
  top-level keys unchanged. `updated` and sport-specific count fields kept for
  backward compatibility. See SCHEMAS.md.

### Deprecated
- `scrapers/mlb/statcast.py` — tombstone. Replaced by `statcast_pitchers.py` and
  `statcast_hitters.py`. Safe to move: 2026-06-01. See file header.
- `scrapers/cbb/evanmiya.py` + `evanmiya_scraper.py` — disabled. Re-enable criteria
  in file headers.
- `scrapers/tennis/sofascore.py` — disabled stub. Re-enable criteria in file header.

---

## [2026-03-25]

### Added
- NHL subsystem: `nhl_api`, `daily_faceoff`, `moneypuck`, `natural_stat_trick` stub.
  GCS paths: `nhl/schedule.json`, `nhl/standings.json`, `nhl/goalies.json`,
  `nhl/lines.json`, `nhl/team_stats.json`, `nhl/goalie_stats.json`,
  `nhl/player_stats.json`.
- `scrapers/nhl/names.py` — canonical NHL team name map. Alias history for Utah Hockey
  Club (Arizona Coyotes → Utah HC 2024-25 → Utah Mammoth 2025-26). `make_join_key()`
  and `normalize_player_name()` helpers.

### Operational
- `natural_stat_trick`: Cloudflare JS challenge blocks requests, headless Playwright,
  and stealth Playwright. Stub returns 0 records with WARNING. To implement: follow
  `torvik_scraper.py` pattern (visible Chromium, no login).
- `daily_faceoff`: ~60s runtime (32 serial HTTP requests, 1.5s delay). Set Cloud Run
  timeout ≥ 180s. Use `fetch_lines: false` for fast goalie-only runs (~5s).

---

## [2026-03-24]

### Added
- `mlb/lineups.json` — MLB Stats API live lineup feed.

### Changed
- MLB season opening day confirmed 2026-03-27. All 8 MLB scrapers writing live to GCS.

### Operational
- MLB Fangraphs and Statcast scrapers return 0 records pre-season. Correct behavior —
  writes empty snapshots with WARNING. Populates on opening day.

---

## [2025-11-01]

### Breaking
- `scrapers/mlb/statcast.py` replaced by `statcast_pitchers.py` and
  `statcast_hitters.py`. Old file raises ImportError. Reason: `parse()` returned a
  grouped dict, violating BaseScraper `list[dict]` contract.