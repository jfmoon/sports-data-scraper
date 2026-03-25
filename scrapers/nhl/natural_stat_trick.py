"""
scrapers/nhl/natural_stat_trick.py

Natural Stat Trick (NST) scraper.
Produces: nhl/team_splits.json, nhl/player_splits.json
Reserves: nhl/nst_lines.json (future)

GCS path ownership:
  nhl/lines.json        → DailyFaceoffScraper (do NOT write here)
  nhl/nst_lines.json    → this scraper (future use)
  nhl/team_splits.json, nhl/player_splits.json → this scraper

BROWSER REQUIREMENT (confirmed 2026-03-25):
  naturalstattrick.com serves a Cloudflare JS challenge — requests returns an
  empty page. This is identical to barttorvik.com behavior.
  Current fetch() uses requests and will get empty responses (0 records).
  Full fix requires the wrapper + standalone Playwright split:
    natural_stat_trick.py          (this file — framework wrapper)
    natural_stat_trick_scraper.py  (standalone Playwright, visible browser)
  following the pattern of torvik_scraper.py / kenpom_scraper.py.

Review history:
  Round 1 (Gemini): initial implementation
  Round 2 (Claude): path collision resolved — nhl/lines.json → nhl/nst_lines.json (Bug 2);
                    SHA-256 content_key (Bug 5)
  Round 3 (both): confirmed correct
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.nhl.names import (
    to_canonical,
    normalize_player_display,
    normalize_player_name,
)

logger = logging.getLogger(__name__)

NST_BASE = "https://www.naturalstattrick.com"
DEFAULT_TIMEOUT = 25
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
RATE_LIMIT_DELAY = 1.5
USER_AGENT = "sports-data-scraper/1.0 (github.com/jfmoon/sports-data-scraper)"
SITUATION_ALL = "all"
SITUATION_5V5 = "5v5"


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class NhlTeamSplit(BaseModel):
    team: str
    season: str
    split_type: str
    situation: str
    games_played: int | None = None
    toi: float | None = None
    cf_pct: float | None = None
    ff_pct: float | None = None
    xgf_pct: float | None = None
    sf_pct: float | None = None
    goals_for: int | None = None
    goals_against: int | None = None
    shooting_pct: float | None = None
    save_pct: float | None = None
    pdo: float | None = None
    hd_cf_pct: float | None = None
    source: str = "natural_stat_trick"
    source_url: str = ""
    fetched_at: str = ""


class NhlPlayerSplit(BaseModel):
    player_name: str
    player_name_norm: str
    team: str
    position: str
    season: str
    split_type: str
    situation: str
    games_played: int | None = None
    toi: float | None = None
    toi_per_game: float | None = None
    goals: int | None = None
    assists: int | None = None
    points: int | None = None
    cf_pct: float | None = None
    xgf_pct: float | None = None
    ixg: float | None = None
    source: str = "natural_stat_trick"
    source_url: str = ""
    fetched_at: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: str | None) -> float | None:
    if val is None or str(val).strip() in ("", "nan", "null", "N/A", "-"):
        return None
    try:
        return float(str(val).replace("%", "").replace(",", ""))
    except ValueError:
        return None


def _safe_int(val: str | None) -> int | None:
    f = _safe_float(val)
    return round(f) if f is not None else None


def _get_with_retry(
    session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT,
    params: dict | None = None,
) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "NST: request failed (attempt %d/%d), retry in %.1fs: %s — %s",
                attempt, MAX_RETRIES, wait, url, exc,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def _season_str(year: int) -> str:
    return f"{year}{year + 1}"


def _parse_nst_table(
    soup: BeautifulSoup,
    table_id: str | None = None,
    required_headers: list[str] | None = None,
) -> list[dict]:
    candidates: list = []
    if table_id:
        t = soup.find("table", {"id": table_id})
        if t:
            candidates = [t]
    if not candidates:
        candidates = soup.find_all("table")
    for table in candidates:
        headers = [th.get_text(strip=True) for th in table.select("thead th")]
        if required_headers:
            headers_lower = {h.lower() for h in headers}
            if not all(rh.lower() in headers_lower for rh in required_headers):
                continue
        if not headers:
            continue
        records = []
        for row in table.select("tbody tr"):
            cells = [td.get_text(strip=True) for td in row.select("td")]
            if len(cells) >= len(headers):
                records.append(dict(zip(headers, cells)))
        if records:
            return records
    if required_headers:
        logger.warning(
            "NST: no table found with required headers %s — page structure may have changed",
            required_headers,
        )
    return []


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class NaturalStatTrickScraper(BaseScraper):

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        s.headers["Accept"] = "text/html,application/xhtml+xml"
        return s

    def _fetched_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _season_year(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year - 1))

    def fetch(self) -> dict:
        season = _season_str(self._season_year())
        logger.warning(
            "NST: naturalstattrick.com requires a visible browser (Cloudflare JS challenge). "
            "Returning empty fetch result. Implement natural_stat_trick_scraper.py to enable. "
            "MoneyPuck (nhl/team_stats.json, nhl/player_stats.json) covers equivalent metrics."
        )
        raw: dict = {"season": str(self._season_year())}
        raw["team_table_5v5"] = None
        raw["team_table_5v5_url"] = f"{NST_BASE}/teamtable.php"
        raw["team_table_all"] = None
        raw["team_table_all_url"] = f"{NST_BASE}/teamtable.php"
        raw["player_table_5v5"] = None
        raw["player_table_5v5_url"] = f"{NST_BASE}/playertable.php"
        return raw

    def content_key(self, raw: dict) -> Any:
        serialized = json.dumps({
            "season": raw.get("season", ""),
            "team_5v5": raw.get("team_table_5v5") or "",
            "team_all": raw.get("team_table_all") or "",
            "player_5v5": raw.get("player_table_5v5") or "",
        }, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _parse_team_table(
        self, html: str | None, situation: str, split_type: str,
        season: str, url: str, fetched_at: str,
    ) -> list[dict]:
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        rows = _parse_nst_table(soup, required_headers=["Team", "GP", "CF%"])
        records = []
        for row in rows:
            team_raw = row.get("Team", "") or row.get("team", "")
            if not team_raw:
                continue
            try:
                team = to_canonical(team_raw)
            except Exception:
                logger.warning("NST team parse: unrecognized team %r", team_raw)
                team = team_raw
            gp = _safe_int(row.get("GP") or row.get("Games"))
            toi = _safe_float(row.get("TOI") or row.get("Ice Time"))
            records.append({
                "team": team, "season": season, "split_type": split_type,
                "situation": situation, "games_played": gp, "toi": toi,
                "cf_pct": _safe_float(row.get("CF%")),
                "ff_pct": _safe_float(row.get("FF%")),
                "xgf_pct": _safe_float(row.get("xGF%") or row.get("Expected Goals %")),
                "sf_pct": _safe_float(row.get("SF%")),
                "goals_for": _safe_int(row.get("GF") or row.get("Goals For")),
                "goals_against": _safe_int(row.get("GA") or row.get("Goals Against")),
                "shooting_pct": _safe_float(row.get("Sh%")),
                "save_pct": _safe_float(row.get("Sv%")),
                "pdo": _safe_float(row.get("PDO")),
                "hd_cf_pct": _safe_float(row.get("HDCF%") or row.get("High Danger CF%")),
                "source": "natural_stat_trick", "source_url": url, "fetched_at": fetched_at,
            })
        return records

    def _parse_player_table(
        self, html: str | None, situation: str, split_type: str,
        season: str, url: str, fetched_at: str,
    ) -> list[dict]:
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        rows = _parse_nst_table(soup, required_headers=["Player", "Position", "GP"])
        records = []
        for row in rows:
            name_raw = row.get("Player", "") or row.get("Name", "")
            if not name_raw:
                continue
            name_display = normalize_player_display(name_raw)
            team_raw = row.get("Team", "") or row.get("team", "")
            try:
                team = to_canonical(team_raw)
            except Exception:
                team = team_raw
            gp = _safe_int(row.get("GP") or row.get("Games"))
            toi = _safe_float(row.get("TOI"))
            toi_per_game = (toi / gp) if (toi and gp) else None
            records.append({
                "player_name": name_display,
                "player_name_norm": normalize_player_name(name_display),
                "team": team, "position": row.get("Position", "") or row.get("Pos", ""),
                "season": season, "split_type": split_type, "situation": situation,
                "games_played": gp, "toi": toi,
                "toi_per_game": round(toi_per_game, 2) if toi_per_game else None,
                "goals": _safe_int(row.get("G") or row.get("Goals")),
                "assists": _safe_int(row.get("A") or row.get("Assists") or row.get("A1")),
                "points": _safe_int(row.get("PTS") or row.get("Points")),
                "cf_pct": _safe_float(row.get("CF%")),
                "xgf_pct": _safe_float(row.get("xGF%")),
                "ixg": _safe_float(row.get("ixG") or row.get("iSCF")),
                "source": "natural_stat_trick", "source_url": url, "fetched_at": fetched_at,
            })
        return records

    def parse(self, raw: dict) -> list[dict]:
        fetched_at = self._fetched_at()
        season = raw["season"]
        team_splits: list[dict] = []
        player_splits: list[dict] = []
        team_splits.extend(self._parse_team_table(
            raw.get("team_table_5v5"), "5v5", "full_season", season,
            raw.get("team_table_5v5_url", ""), fetched_at,
        ))
        team_splits.extend(self._parse_team_table(
            raw.get("team_table_all"), "all", "full_season", season,
            raw.get("team_table_all_url", ""), fetched_at,
        ))
        player_splits.extend(self._parse_player_table(
            raw.get("player_table_5v5"), "5v5", "full_season", season,
            raw.get("player_table_5v5_url", ""), fetched_at,
        ))
        return [
            {"_type": "team_splits", "records": team_splits},
            {"_type": "player_splits", "records": player_splits},
        ]

    def validate(self, records: list[dict]) -> list[BaseModel]:
        validated: list[BaseModel] = []
        for batch in records:
            if batch["_type"] == "team_splits":
                for r in batch["records"]:
                    validated.append(NhlTeamSplit(**r))
            elif batch["_type"] == "player_splits":
                for r in batch["records"]:
                    validated.append(NhlPlayerSplit(**r))
        return validated

    def upsert(self, records: list[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        fetched_at = self._fetched_at()
        season = str(self._season_year())

        team_splits = [r.model_dump() for r in records if isinstance(r, NhlTeamSplit)]
        player_splits = [r.model_dump() for r in records if isinstance(r, NhlPlayerSplit)]

        if team_splits:
            ts_payload = {
                # Standard envelope — schema_version 1
                "schema_version": 1,
                "generated_at": fetched_at,
                "scraper_key": "natural_stat_trick",
                "record_count": len(team_splits),
                "warnings": [],  # TODO: propagate scraper warnings here
                # Existing fields — unchanged
                "updated": fetched_at,
                "season": season,
                "team_splits": team_splits,
            }
            storage.persist_raw(source="nst_team_splits", data=ts_payload)
            ts_gcs = self.config.get("team_splits_gcs_object", "nhl/team_splits.json")
            storage.write_json(ts_gcs, ts_payload)
            logger.info("NST: wrote %d team split records to %s", len(team_splits), ts_gcs)

        if player_splits:
            ps_payload = {
                # Standard envelope — schema_version 1
                "schema_version": 1,
                "generated_at": fetched_at,
                "scraper_key": "natural_stat_trick",
                "record_count": len(player_splits),
                "warnings": [],  # TODO: propagate scraper warnings here
                # Existing fields — unchanged
                "updated": fetched_at,
                "season": season,
                "player_splits": player_splits,
            }
            storage.persist_raw(source="nst_player_splits", data=ps_payload)
            ps_gcs = self.config.get("player_splits_gcs_object", "nhl/player_splits.json")
            storage.write_json(ps_gcs, ps_payload)
            logger.info("NST: wrote %d player split records to %s", len(player_splits), ps_gcs)

        # nhl/nst_lines.json — reserved for future NST line report fetch.
        # Intentionally does NOT write to nhl/lines.json (owned by DailyFaceoffScraper).
        nst_lines_gcs = self.config.get("gcs_object", "nhl/nst_lines.json")
        logger.info(
            "NST: nst_lines path registered at %s (line report fetch not yet implemented)",
            nst_lines_gcs,
        )
