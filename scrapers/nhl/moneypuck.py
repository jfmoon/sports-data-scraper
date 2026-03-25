"""
scrapers/nhl/moneypuck.py

MoneyPuck advanced stats scraper.
Produces: nhl/team_stats.json, nhl/goalie_stats.json, nhl/player_stats.json

CSV endpoints (public, no auth):
  Teams:   https://moneypuck.com/moneypuck/playerData/seasonSummary/{season}/regular/teams.csv
  Goalies: https://moneypuck.com/moneypuck/playerData/seasonSummary/{season}/regular/goalies.csv
  Skaters: https://moneypuck.com/moneypuck/playerData/seasonSummary/{season}/regular/skaters.csv

Extracts "all" and "5on5" situations only.

Review history:
  Round 1 (Gemini): initial implementation
  Round 2 (Claude): imports moved to top (Bug 3); SHA-256 content_key (Bug 5);
                    _safe_int uses round() with tolerance warning (Bug 6)
  Round 3 (both):   confirmed correct — _safe_int policy note documented
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.nhl.names import (
    to_canonical,
    normalize_player_display,
    normalize_player_name,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MONEYPUCK_BASE = "https://moneypuck.com/moneypuck/playerData/seasonSummary"
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
USER_AGENT = "sports-data-scraper/1.0 (github.com/jfmoon/sports-data-scraper)"
MIN_TOI_SKATER = 50.0       # minimum TOI (minutes) to include a skater
MIN_STARTS_GOALIE = 5       # minimum starts to include a goalie

TARGET_SITUATIONS = {"all", "5on5"}

# Tolerance for emitting a warning when a float count field is not close to an integer.
# Values within this range of their rounded form are treated silently (e.g. "1.0", "2.0000001").
# Values outside this range round but emit a WARNING so anomalies are visible in logs.
# Policy: Extract layer prefers resilience (round + warn) over nulling. If stricter behavior
# is needed, change the except block to return None when abs(f - rounded) > _INT_TOLERANCE.
_INT_TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class NhlTeamStats(BaseModel):
    team: str
    season: str
    situation: str                      # "all" | "5on5"
    games_played: int | None = None
    toi: float | None = None            # total time on ice (minutes)
    xgf: float | None = None
    xga: float | None = None
    xgf_pct: float | None = None        # xGF%
    xgf_per_60: float | None = None
    xga_per_60: float | None = None
    cf: float | None = None
    ca: float | None = None
    cf_pct: float | None = None         # CF%
    ff_pct: float | None = None         # FF%
    sf_pct: float | None = None         # SF%
    shooting_pct: float | None = None
    save_pct: float | None = None
    pdo: float | None = None
    hd_cf_pct: float | None = None      # high-danger Corsi%
    hd_goals_for: float | None = None
    hd_goals_against: float | None = None
    rush_cf_pct: float | None = None
    rebound_cf_pct: float | None = None
    pp_toi_pct: float | None = None
    pk_toi_pct: float | None = None
    source: str = "moneypuck"
    source_url: str = ""
    fetched_at: str = ""


class NhlGoalieStat(BaseModel):
    goalie_name: str
    goalie_name_norm: str
    team: str
    season: str
    situation: str                      # "all" | "5on5"
    games_played: int | None = None
    starts: int | None = None
    toi: float | None = None            # minutes
    goals_against: int | None = None
    saves: int | None = None
    save_pct: float | None = None
    goals_saved_above_expected: float | None = None   # GSAx
    goals_saved_above_average: float | None = None    # GSAA
    xga: float | None = None
    hd_save_pct: float | None = None
    md_save_pct: float | None = None
    ld_save_pct: float | None = None
    source: str = "moneypuck"
    source_url: str = ""
    fetched_at: str = ""


class NhlSkaterStat(BaseModel):
    player_name: str
    player_name_norm: str
    team: str
    position: str
    season: str
    situation: str                      # "all" | "5on5"
    games_played: int | None = None
    toi: float | None = None            # minutes
    toi_per_game: float | None = None
    goals: int | None = None
    assists: int | None = None
    points: int | None = None
    shots: int | None = None
    shots_per_game: float | None = None
    ixg: float | None = None
    ixg_per_60: float | None = None
    on_ice_xgf_pct: float | None = None
    on_ice_cf_pct: float | None = None
    source: str = "moneypuck"
    source_url: str = ""
    fetched_at: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: str | None) -> float | None:
    if val is None or val == "" or str(val).lower() in ("nan", "null", "n/a"):
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _safe_int(val: str | None, field_name: str = "") -> int | None:
    """Parse val to int via float, using round() to avoid silent truncation.

    Why round() not int(): int(1.9) → 1 (silent data corruption).
    round(1.9) → 2, which is the correct integer representation.

    Anomaly detection: if the raw float is more than _INT_TOLERANCE from its
    rounded value (e.g. "1.49" which rounds to 1), a WARNING is emitted so
    the anomaly is visible in logs. The rounded value is still returned.

    Extract-layer policy: prefer resilience (round + warn) over nulling.
    If strict behavior is needed, return None instead of rounded when
    abs(f - rounded) > _INT_TOLERANCE.
    """
    f = _safe_float(val)
    if f is None:
        return None
    rounded = round(f)
    if abs(f - rounded) > _INT_TOLERANCE:
        logger.warning(
            "_safe_int: unexpected non-integer float %s rounded to %d "
            "(field=%r, raw=%r). Check source data.",
            f, rounded, field_name, val,
        )
    return rounded


def _get_with_retry(
    session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT
) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "MoneyPuck: request failed (attempt %d/%d), retry in %.1fs: %s — %s",
                attempt, MAX_RETRIES, wait, url, exc,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")  # pragma: no cover


def _read_csv(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def _sha256_content_key(payload: dict) -> str:
    """Return SHA-256 hex digest of a JSON-serialized dict.

    Uses sort_keys=True and separators=(",", ":") for a deterministic,
    whitespace-free serialization. Safe for str/None values.
    """
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class MoneypuckScraper(BaseScraper):
    """Scrapes MoneyPuck for team, goalie, and skater advanced stats."""

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        return s

    def _fetched_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _season(self) -> str:
        """Season year string, e.g. '2025' for the 2025-26 season."""
        return str(self.config.get("season", datetime.now(timezone.utc).year - 1))

    def _url(self, entity: str) -> str:
        return f"{MONEYPUCK_BASE}/{self._season()}/regular/{entity}.csv"

    # ----- fetch ------------------------------------------------------------

    def fetch(self) -> dict:
        """Fetch teams, goalies, and skaters CSVs independently.

        Each entity is soft-failed: if one CSV request fails, the others
        still proceed. None is stored for failed entities and produces
        empty parse output for that entity type.
        """
        session = self._session()
        season = self._season()
        raw: dict = {}

        for entity in ("teams", "goalies", "skaters"):
            url = self._url(entity)
            logger.info("MoneyPuck: fetching %s from %s", entity, url)
            try:
                resp = _get_with_retry(session, url)
                raw[entity] = resp.text
                raw[f"{entity}_url"] = url
                time.sleep(0.5)
            except requests.RequestException as exc:
                logger.error("MoneyPuck: failed to fetch %s: %s", entity, exc)
                raw[entity] = None
                raw[f"{entity}_url"] = url

        raw["season"] = season
        return raw

    # ----- content_key ------------------------------------------------------

    def content_key(self, raw: dict) -> Any:
        """SHA-256 hash of full CSV content for all three datasets.

        Hashing the entire payload (not a prefix) ensures that mid-file row
        changes — e.g. updated stats for a player listed 300+ rows in — still
        trigger a re-scrape. Season string is included to prevent cross-season
        hash collisions on empty or single-row CSVs.

        json.dumps with sort_keys=True and separators=(",", ":") produces
        a deterministic, whitespace-free serialization.
        """
        return _sha256_content_key({
            "season": raw.get("season", ""),
            "teams": raw.get("teams") or "",
            "goalies": raw.get("goalies") or "",
            "skaters": raw.get("skaters") or "",
        })

    # ----- parse teams ------------------------------------------------------

    def _parse_teams(
        self, csv_text: str, season: str, url: str, fetched_at: str
    ) -> list[dict]:
        if not csv_text:
            logger.warning("MoneyPuck: no teams CSV data")
            return []
        rows = _read_csv(csv_text)
        records: list[dict] = []
        for row in rows:
            situation = row.get("situation", "").lower()
            if situation not in TARGET_SITUATIONS:
                continue

            team_raw = row.get("team", "")
            try:
                team = to_canonical(team_raw)
            except Exception:
                logger.warning("MoneyPuck teams: unrecognized team %r", team_raw)
                team = team_raw

            records.append({
                "team": team,
                "season": season,
                "situation": situation,
                "games_played": _safe_int(row.get("games_played") or row.get("gamesPlayed"), "games_played"),
                "toi": _safe_float(row.get("icetime") or row.get("toi")),
                "xgf": _safe_float(row.get("xGoalsFor") or row.get("xgf")),
                "xga": _safe_float(row.get("xGoalsAgainst") or row.get("xga")),
                "xgf_pct": _safe_float(row.get("xGoalsPercentage") or row.get("xGF%")),
                "xgf_per_60": _safe_float(row.get("xGoalsForPer60") or row.get("xgf_per_60")),
                "xga_per_60": _safe_float(row.get("xGoalsAgainstPer60") or row.get("xga_per_60")),
                "cf": _safe_float(row.get("corsiFor") or row.get("cf")),
                "ca": _safe_float(row.get("corsiAgainst") or row.get("ca")),
                "cf_pct": _safe_float(row.get("corsiPercentage") or row.get("CF%")),
                "ff_pct": _safe_float(row.get("fenwickPercentage") or row.get("FF%")),
                "sf_pct": _safe_float(row.get("shotsForPercentage") or row.get("SF%")),
                "shooting_pct": _safe_float(row.get("shootingPct") or row.get("onIceShootingPct")),
                "save_pct": _safe_float(row.get("savePct") or row.get("onIceSavePct")),
                "pdo": _safe_float(row.get("pdo") or row.get("PDO")),
                "hd_cf_pct": _safe_float(row.get("highDangerCorsiPercentage") or row.get("hdCF%")),
                "hd_goals_for": _safe_float(row.get("highDangerGoalsFor")),
                "hd_goals_against": _safe_float(row.get("highDangerGoalsAgainst")),
                "rush_cf_pct": _safe_float(row.get("rushCorsiPercentage")),
                "rebound_cf_pct": _safe_float(row.get("reboundCorsiPercentage")),
                "pp_toi_pct": _safe_float(row.get("ppToiPct")),
                "pk_toi_pct": _safe_float(row.get("pkToiPct")),
                "source": "moneypuck",
                "source_url": url,
                "fetched_at": fetched_at,
            })
        return records

    # ----- parse goalies ----------------------------------------------------

    def _parse_goalies(
        self, csv_text: str, season: str, url: str, fetched_at: str
    ) -> list[dict]:
        if not csv_text:
            logger.warning("MoneyPuck: no goalies CSV data")
            return []
        rows = _read_csv(csv_text)
        records: list[dict] = []
        for row in rows:
            situation = row.get("situation", "").lower()
            if situation not in TARGET_SITUATIONS:
                continue

            starts = _safe_int(row.get("starts") or row.get("gamesStarted"), "starts")
            if starts is not None and starts < MIN_STARTS_GOALIE:
                continue

            name_raw = row.get("name", "") or row.get("playerName", "")
            name_display = normalize_player_display(name_raw)

            team_raw = row.get("team", "")
            try:
                team = to_canonical(team_raw)
            except Exception:
                logger.warning("MoneyPuck goalies: unrecognized team %r", team_raw)
                team = team_raw

            records.append({
                "goalie_name": name_display,
                "goalie_name_norm": normalize_player_name(name_display),
                "team": team,
                "season": season,
                "situation": situation,
                "games_played": _safe_int(row.get("games_played") or row.get("gamesPlayed"), "games_played"),
                "starts": starts,
                "toi": _safe_float(row.get("icetime") or row.get("toi")),
                "goals_against": _safe_int(row.get("goalsAgainst"), "goals_against"),
                "saves": _safe_int(row.get("saves"), "saves"),
                "save_pct": _safe_float(row.get("savePct") or row.get("save%")),
                "goals_saved_above_expected": _safe_float(
                    row.get("goalsSavedAboveExpected") or row.get("GSAx")
                ),
                "goals_saved_above_average": _safe_float(
                    row.get("goalsSavedAboveAverage") or row.get("GSAA")
                ),
                "xga": _safe_float(row.get("xGoalsAgainst") or row.get("xGA")),
                "hd_save_pct": _safe_float(row.get("highDangerSavePct") or row.get("hdSave%")),
                "md_save_pct": _safe_float(row.get("mediumDangerSavePct") or row.get("mdSave%")),
                "ld_save_pct": _safe_float(row.get("lowDangerSavePct") or row.get("ldSave%")),
                "source": "moneypuck",
                "source_url": url,
                "fetched_at": fetched_at,
            })
        return records

    # ----- parse skaters ----------------------------------------------------

    def _parse_skaters(
        self, csv_text: str, season: str, url: str, fetched_at: str
    ) -> list[dict]:
        if not csv_text:
            logger.warning("MoneyPuck: no skaters CSV data")
            return []
        rows = _read_csv(csv_text)
        records: list[dict] = []
        for row in rows:
            situation = row.get("situation", "").lower()
            if situation not in TARGET_SITUATIONS:
                continue

            toi = _safe_float(row.get("icetime") or row.get("toi"))
            if toi is not None and toi < MIN_TOI_SKATER:
                continue

            name_raw = row.get("name", "") or row.get("playerName", "")
            name_display = normalize_player_display(name_raw)

            team_raw = row.get("team", "")
            try:
                team = to_canonical(team_raw)
            except Exception:
                logger.warning("MoneyPuck skaters: unrecognized team %r", team_raw)
                team = team_raw

            gp = _safe_int(row.get("games_played") or row.get("gamesPlayed"), "games_played")
            toi_per_game = (toi / gp) if (toi and gp) else None
            shots = _safe_int(row.get("shotsOnGoalFor") or row.get("shots"), "shots")
            shots_per_game = (shots / gp) if (shots and gp) else None
            ixg = _safe_float(row.get("ixG") or row.get("individualExpectedGoals"))
            toi_per_60 = toi / 60 if toi else None
            ixg_per_60 = (ixg / toi_per_60) if (ixg is not None and toi_per_60) else None

            records.append({
                "player_name": name_display,
                "player_name_norm": normalize_player_name(name_display),
                "team": team,
                "position": row.get("position", ""),
                "season": season,
                "situation": situation,
                "games_played": gp,
                "toi": toi,
                "toi_per_game": round(toi_per_game, 2) if toi_per_game else None,
                "goals": _safe_int(row.get("goalsFor") or row.get("goals"), "goals"),
                "assists": _safe_int(row.get("assistsFor") or row.get("assists"), "assists"),
                "points": _safe_int(row.get("pointsFor") or row.get("points"), "points"),
                "shots": shots,
                "shots_per_game": round(shots_per_game, 2) if shots_per_game else None,
                "ixg": ixg,
                "ixg_per_60": round(ixg_per_60, 3) if ixg_per_60 else None,
                "on_ice_xgf_pct": _safe_float(row.get("onIce_xGoalsPercentage") or row.get("xGF%")),
                "on_ice_cf_pct": _safe_float(row.get("onIce_corsiPercentage") or row.get("CF%")),
                "source": "moneypuck",
                "source_url": url,
                "fetched_at": fetched_at,
            })
        return records

    # ----- parse (main) -----------------------------------------------------

    def parse(self, raw: dict) -> list[dict]:
        season = raw["season"]
        fetched_at = self._fetched_at()
        return [
            {"_type": "teams",   "records": self._parse_teams(raw.get("teams") or "",   season, raw.get("teams_url", ""),   fetched_at)},
            {"_type": "goalies", "records": self._parse_goalies(raw.get("goalies") or "", season, raw.get("goalies_url", ""), fetched_at)},
            {"_type": "skaters", "records": self._parse_skaters(raw.get("skaters") or "", season, raw.get("skaters_url", ""), fetched_at)},
        ]

    # ----- validate ---------------------------------------------------------

    def validate(self, records: list[dict]) -> list[BaseModel]:
        validated: list[BaseModel] = []
        for batch in records:
            if batch["_type"] == "teams":
                for r in batch["records"]:
                    validated.append(NhlTeamStats(**r))
            elif batch["_type"] == "goalies":
                for r in batch["records"]:
                    validated.append(NhlGoalieStat(**r))
            elif batch["_type"] == "skaters":
                for r in batch["records"]:
                    validated.append(NhlSkaterStat(**r))
        return validated

    # ----- upsert -----------------------------------------------------------

    def upsert(self, records: list[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        fetched_at = self._fetched_at()
        season = self._season()

        teams = [r.model_dump() for r in records if isinstance(r, NhlTeamStats)]
        goalies = [r.model_dump() for r in records if isinstance(r, NhlGoalieStat)]
        skaters = [r.model_dump() for r in records if isinstance(r, NhlSkaterStat)]

        for payload_data, gcs_key, source_name, count_field in [
            (
                {"updated": fetched_at, "season": season,
                 "situations": list(TARGET_SITUATIONS),
                 "team_count": len({r["team"] for r in teams}),
                 "record_count": len(teams), "team_stats": teams},
                self.config.get("team_stats_gcs_object", "nhl/team_stats.json"),
                "moneypuck_teams",
                len(teams),
            ),
            (
                {"updated": fetched_at, "season": season,
                 "situations": list(TARGET_SITUATIONS),
                 "goalie_count": len({r["goalie_name"] for r in goalies}),
                 "record_count": len(goalies), "goalie_stats": goalies},
                self.config.get("goalie_stats_gcs_object", "nhl/goalie_stats.json"),
                "moneypuck_goalies",
                len(goalies),
            ),
            (
                {"updated": fetched_at, "season": season,
                 "situations": list(TARGET_SITUATIONS),
                 "player_count": len({r["player_name"] for r in skaters}),
                 "record_count": len(skaters), "player_stats": skaters},
                self.config.get("player_stats_gcs_object", "nhl/player_stats.json"),
                "moneypuck_skaters",
                len(skaters),
            ),
        ]:
            storage.persist_raw(source=source_name, data=payload_data)
            storage.write_json(gcs_key, payload_data)
            logger.info("MoneyPuck: wrote %d records to %s", count_field, gcs_key)
