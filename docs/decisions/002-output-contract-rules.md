# ADR 002 — GCS Output Contract Rules

**Date:** 2026-03-25  
**Status:** Accepted  
**Applies to:** All scrapers in `jfmoon/sports-data-scraper`

---

## Context

This repo is the Extract layer of a three-tier ETL pipeline. Every JSON file
written to `gs://sports-data-scraper-491116/` is read by `jfmoon/sports-analysis`
(the Transform layer). Any change to a GCS output shape that is not coordinated
with the Transform layer will silently break downstream consumers — there is no
schema enforcement at the GCS boundary, only Pydantic models in the analysis repo.

As of 2026-03-25, `schema_version: 1` has been added to all active scraper
payloads as the baseline. This document defines the rules for all future changes.

---

## Definitions

### Additive change
A change that adds new information without removing or altering existing
information. Existing consumers continue to work without any code changes.

Examples:
- Adding a new optional field to a payload (e.g. `"broadcast": "ESPN"`)
- Adding a new envelope field (e.g. `schema_version` itself when it was introduced)
- Adding a new GCS path that did not previously exist

### Breaking change
A change that alters, removes, or renames existing fields such that a consumer
reading the old shape will either fail or silently receive wrong data.

Examples:
- Renaming `game_count` to `total_games`
- Removing a field the analysis layer reads
- Changing a field type (e.g. `game_id` from `str` to `int`)
- Changing nesting (e.g. moving a field inside a sub-object)
- Changing the number of `write_json` calls in a scraper's `upsert()`

---

## Rules

### 1. `schema_version` increment policy

Increment `schema_version` on **any** shape change, additive or breaking. The
integer always goes up, never down.

- Additive change: increment, update `SCHEMAS.md`, no consumer coordination required
- Breaking change: increment, update `SCHEMAS.md`, add entry to `MIGRATIONS.md`,
  coordinate with `jfmoon/sports-analysis` before merging

The version is an integer field on the payload. For MLB scrapers using Pydantic
Snapshot models, update the `schema_version` default value on the model class.
For CBB/NHL scrapers using plain dicts, update the hardcoded value in `upsert()`.

### 2. Updating `SCHEMAS.md`

Every shape change — additive or breaking — requires a `SCHEMAS.md` update before
the PR is merged. The update must include:

- The new or changed field in the field table for that GCS path
- A new line in the `schema_version history` block for that path

`SCHEMAS.md` documents **actual current shapes only**. Do not document aspirational
fields or fields that exist in a branch but not yet in `main`.

### 3. Breaking changes require `MIGRATIONS.md`

`MIGRATIONS.md` does not yet exist. Create it when the first breaking change occurs.
Each entry must include:

- Date
- GCS path affected
- What changed (before/after shapes)
- Which `schema_version` introduced the change
- What the analysis layer must update

### 4. `scraper_key` must match the registry

Every payload written to GCS must include a `scraper_key` field whose value exactly
matches the key used for that scraper in `registry.py` and `config.yaml`.

For `scrapers/sports/action_network.py`, which handles four sports under four
separate registry keys, each sport's payload uses its own key:
`nba_odds`, `mlb_odds`, `nhl_odds`, `nfl_odds`.

The `scraper_key` value is derived from `self.config["name"]` at runtime, which
reads the `name:` field from the config.yaml scraper block. This is the authoritative
source — do not hardcode a different value.

### 5. Do not change GCS paths without a MIGRATIONS.md entry

Moving a GCS path (e.g. renaming `nhl/lines.json` to `nhl/line_combos.json`) is a
breaking change even if the field contents are identical. The analysis layer reads
specific blob names.

### 6. Do not change the number of `write_json` calls in `upsert()`

Scrapers like `nhl_api`, `daily_faceoff`, and `moneypuck` make multiple `write_json`
calls in a single `upsert()`. Adding or removing a call changes what GCS paths are
written on each run. This is a breaking change and requires a `MIGRATIONS.md` entry.

### 7. Legacy compatibility fields

`updated` and sport-specific count fields (`game_count`, `team_count`, etc.) are
kept alongside the standard envelope fields (`generated_at`, `record_count`) for
backward compatibility. Do not remove them until `jfmoon/sports-analysis` confirms
it has migrated to the envelope fields.

---

## Standard Envelope (schema_version 1)

All active scraper payloads include these five fields as of 2026-03-25:

```python
"schema_version": 1,          # int — increments on any shape change
"generated_at": "<ISO8601>",   # str — UTC timestamp, same format as updated
"scraper_key": "<key>",        # str — exact registry.py key for this scraper
"record_count": N,             # int — count of records in primary data array
"warnings": [],                # list[str] — non-fatal issues (empty until wired)
```

These fields appear alongside all pre-existing payload keys. Existing keys are not
renamed or removed.

---

## Checklist for any output shape change

Before opening a PR that changes a GCS payload shape:

- [ ] `schema_version` incremented in the relevant scraper file
- [ ] `SCHEMAS.md` updated — field table and version history block
- [ ] If breaking: `MIGRATIONS.md` entry added
- [ ] If breaking: coordination with `jfmoon/sports-analysis` confirmed
- [ ] `CHANGELOG.md` entry added under `### Changed` or `### Breaking`
