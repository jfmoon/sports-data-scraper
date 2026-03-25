# GCS Output Schemas

Authoritative reference for all JSON files written to `gs://sports-data-scraper-491116/`.

**How to read this document:**
- Fields are documented exactly as currently written to GCS. No idealized shapes.
- Do not infer missing fields. If a field is not listed here, it is not guaranteed present.
- Envelope fields are the five standard fields added in PR #1 (schema_version 1).
  All other fields predate schema versioning and are sport-specific.
- Consumers must treat any unlisted field as unstable.

**Updating this document:**
- Any field addition, rename, removal, or type change requires a `schema_version` increment
  and an update to the relevant section below.
- Breaking changes (rename/removal) also require an entry in MIGRATIONS.md.
- `scraper_key` values must exactly match `registry.py`.

---

## Standard Envelope Fields

Present in all outputs as of schema_version 1.

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | Starts at 1. Increments on any shape change. |
| `generated_at` | string (ISO8601 UTC) | Timestamp of this write. Same format as `updated` in this repo. |
| `scraper_key` | string | Exact `registry.py` key that produced this file. |
| `record_count` | int | Count of records in the primary data array for this specific payload. |
| `warnings` | list[string] | Non-fatal issues from this run. `[]` until warning capture is wired. |

## Legacy Compatibility Fields

Also present in all outputs. Kept for backward compatibility with existing consumers.
Do not remove until downstream consumers confirm migration.

| Field | Notes |
|---|---|
| `updated` | Legacy timestamp. Identical value to `generated_at`. Going-forward standard is `generated_at`. |
| Sport-specific count fields (`game_count`, `team_count`, `player_count`, etc.) | Kept alongside `record_count`. Values are identical. |

---

## CBB

### `cbb/kenpom.json`
**Scraper key:** `<exact registry key>`  
**Cadence:** Manual / local only (visible browser, manual login required)  
**schema_version:** 1 (since YYYY-MM-DD)

Top-level structure:
```json
{
  "schema_version": 1,
  "generated_at": "...",
  "scraper_key": "...",
  "record_count": 68,
  "warnings": [],
  "updated": "...",
  "team_count": 68,
  "teams": [...]
}
```

`teams` object:

| Field | Type | Notes |
|---|---|---|
| `name` | string | Canonical per `scrapers/cbb/names.py` |
| `kenpom_rank` | int | |
| `adj_o` | float | |
| `adj_d` | float | |
| `adj_t` | float | |
| `three_p_pct` | float | |
| `three_par` | float | |
| `ftr` | float | |
| `to_pct` | float | |
| `steal_pct` | float | |
| `block_pct` | float | |
| `orb_pct` | float | |
| `opp_3p_pct` | float | |
| `experience` | float | |
| `source` | string | Always `"kenpom"` |
| `fetched_at` | string (ISO8601) | |

schema_version history:
- v1 (YYYY-MM-DD): Initial versioned release.

---

[Repeat this section structure for every active GCS path]

---

## Reserved Paths (writes 0 records)

| GCS Path | Scraper key | Status |
|---|---|---|
| `nhl/team_splits.json` | `natural_stat_trick` | Cloudflare blocked — empty payload, 0 records |
| `nhl/player_splits.json` | `natural_stat_trick` | Same |
| `nhl/nst_lines.json` | `natural_stat_trick` | Same |