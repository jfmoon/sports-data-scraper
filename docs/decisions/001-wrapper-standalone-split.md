# ADR 001 — Wrapper / Standalone Split for Browser-Based Scrapers

**Date:** 2026-03-25  
**Status:** Accepted  
**Applies to:** KenPom, Torvik, TennisAbstract, EvanMiya, Natural Stat Trick (pending)

---

## Context

Several data sources require a real browser to scrape successfully. Three distinct
problems drove this decision:

1. **Cloudflare JS challenges** block `requests`, headless Playwright, and
   `playwright-stealth` (Torvik, Natural Stat Trick). Only a visible Chromium
   instance passes.
2. **Manual login** is required for subscription-gated sources (KenPom, EvanMiya).
   The script must pause and wait for a human to complete the login flow.
3. **Fresh browser per request** is required for TennisAbstract due to Cloudflare
   session tracking at the browser context level. Sharing any context across players
   causes blocks after the first successful load.

None of these constraints fit cleanly inside the standard `BaseScraper.fetch()`
lifecycle, which assumes a single synchronous fetch call that returns data.

---

## Decision

Browser-based scrapers are split into two files:

| File | Role | When to edit |
|---|---|---|
| `{name}_scraper.py` | **Standalone** — contains all browser automation, Playwright setup, page parsing, and data extraction logic. Can be run directly from the command line for testing. | When fixing a parse bug, changing selectors, updating the scrape flow, or debugging a Cloudflare workaround. |
| `{name}.py` | **Framework wrapper** — implements the `BaseScraper` contract (`fetch`, `content_key`, `parse`, `validate`, `upsert`). Calls the standalone via `subprocess.run()` or direct import. Handles GCS paths and StorageManager writes. | When changing GCS output paths, modifying the Pydantic model, or adjusting framework integration. |

**Do not mix concerns.** If you're fixing a parse issue in how KenPom table rows
are extracted, edit `kenpom_scraper.py`. If you're changing where the output is
written in GCS, edit `kenpom.py`.

---

## Why `subprocess.run()` for KenPom

KenPom's scraper must open a visible browser and pause for manual login. If the
scraper were called as a Python import inside the framework wrapper, the login
pause would block the ScraperRunner process entirely. Running it as a subprocess
lets the human interact with the browser in a separate process while the parent
waits for completion.

Torvik and EvanMiya are called via direct Python import rather than subprocess
because they do not require interactive login — the visible browser runs
autonomously once launched.

TennisAbstract uses subprocess for the full ranked-list scrape (long-running,
~60–80 min) and for individual player scrapes, which isolates each browser
lifetime to its own process.

---

## Scrapers currently using this pattern

| Scraper | Standalone | Wrapper | Login required | Browser mode |
|---|---|---|---|---|
| KenPom | `scrapers/cbb/kenpom_scraper.py` | `scrapers/cbb/kenpom.py` | Yes — manual | Visible Chromium |
| Torvik | `scrapers/cbb/torvik_scraper.py` | `scrapers/cbb/torvik.py` | No | Visible Chromium |
| TennisAbstract | `scrapers/tennis/tennisabstract_scraper.py` | `scrapers/tennis/tennisabstract.py` | No | Headless + stealth |
| EvanMiya | `scrapers/cbb/evanmiya_scraper.py` | `scrapers/cbb/evanmiya.py` | Yes — manual | Visible Chromium |

---

## Natural Stat Trick (pending)

`scrapers/nhl/natural_stat_trick.py` currently exists as a framework wrapper stub
that returns 0 records with a WARNING. It is Cloudflare-blocked via `requests`,
headless Playwright, and stealth Playwright — identical behavior to Torvik.

When implemented, it must follow this same pattern:
- Create `scrapers/nhl/natural_stat_trick_scraper.py` — visible Chromium, no login
- Follow `torvik_scraper.py` exactly for browser launch and page interaction
- The existing `natural_stat_trick.py` wrapper calls the standalone and handles GCS

MoneyPuck covers equivalent metrics in the meantime.

---

## Consequences

- Any developer editing a browser scraper must open both files to understand the
  full picture. The standalone handles *how* data is fetched; the wrapper handles
  *where* it goes.
- New browser-based scrapers must always create both files. A standalone-only file
  is not registered in the framework. A wrapper-only file without a standalone has
  no working fetch logic.
- The `requirements.txt` does not include `playwright` or `playwright-stealth`.
  These must be installed separately on any machine running browser scrapers.
