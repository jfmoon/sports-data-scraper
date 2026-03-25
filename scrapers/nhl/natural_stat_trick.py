"""
scrapers/nhl/natural_stat_trick.py

Framework wrapper for the Natural Stat Trick standalone scraper.
Produces: nhl/team_splits.json, nhl/player_splits.json
Reserves:  nhl/nst_lines.json (future — line report not yet implemented)

GCS path ownership:
  nhl/lines.json      → DailyFaceoffScraper (do NOT write here)
  nhl/nst_lines.json  → this scraper (future use)
  nhl/team_splits.json, nhl/player_splits.json → this scraper

Why wrapper + standalone split:
  naturalstattrick.com serves a Cloudflare JS challenge that blocks requests,
  headless Playwright, and playwright-stealth. A visible Chromium browser is
  required — identical to barttorvik.com / Torvik pattern.
  The standalone scraper (natural_stat_trick_scraper.py) is invoked via
  subprocess.run() so it can open a visible browser window while the framework
  wrapper handles GCS writes, dedup, and validation.

BaseScraper contract:
  fetch()       → invokes standalone scraper subprocess, returns raw JSON str
  content_key() → MD5 of stable data slice (excludes timestamps)
  parse()       → list[dict] batch envelopes (team_splits, player_splits)
  validate()    → list[NhlTeamSplit | NhlPlayerSplit]
  upsert()      → writes team_splits.json + player_splits.json + raw archives
"""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

from pydantic import BaseModel, ConfigDict

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.nhl.names import to_canonical, normalize_player_name

logger = logging.getLogger(__name__)


def _mmss_to_float(val: object) -> float | None:
    """Convert MM:SS string to total minutes float. Returns None if unparseable."""
    if val is None:
        return None
    s = str(val).strip()
    if ":" in s:
        try:
            mm, ss = s.split(":", 1)
            return round(int(mm) + int(ss) / 60, 4)
        except (ValueError, TypeError):
            pass
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class NhlTeamSplit(BaseModel):
    """Team-level NST split record.

    extra='allow' because NST exposes many stat columns that vary by season
    and situation. Required identity fields are season, team, split, gp.
    toi is optional because NST uses different column names (toi vs toi_per_gp)
    depending on the rate= parameter. The Transform layer selects what it needs.
    """
    model_config = ConfigDict(extra="allow")

    season: int
    team: str
    split: str                   # "5v5" | "all"
    gp: int
    toi: float | None = None


class NhlPlayerSplit(BaseModel):
    """Player-level NST split record.

    Same extra='allow' policy as NhlTeamSplit.
    player_name_norm is the Unicode-normalized comparison key for cross-source joins.
    """
    model_config = ConfigDict(extra="allow")

    season: int
    team: str
    player: str                  # display name
    player_name_norm: str        # normalized comparison key
    split: str                   # "5v5"
    gp: int
    toi: float


# ---------------------------------------------------------------------------
# Wrapper scraper
# ---------------------------------------------------------------------------

class NaturalStatTrickScraper(BaseScraper):
    """Framework wrapper for the NST Playwright standalone scraper.

    Invokes natural_stat_trick_scraper.py as a subprocess so the visible
    browser requirement does not affect the framework's process model.
    """

    source_key = "natural_stat_trick"
    schema_version = 1

    def _check_config(self) -> None:
        if "season" not in self.config:
            raise KeyError("NaturalStatTrickScraper config missing required 'season' key.")

    # ----- fetch ------------------------------------------------------------

    def fetch(self) -> str:
        """Invoke the standalone scraper as a subprocess.

        The standalone scraper opens a visible Chromium window, visits NST,
        extracts team and player tables, and writes a JSON payload to a temp
        file. This method reads that file and returns the raw JSON string.

        Raises RuntimeError if the subprocess exits non-zero.
        """
        self._check_config()
        season = self.config["season"]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            cmd = [
                sys.executable,
                "-m", "scrapers.nhl.natural_stat_trick_scraper",
                "--season", str(season),
                "--out", tmp_path,
            ]
            logger.info("Launching NST standalone scraper: %s", " ".join(cmd))

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,   # 10-minute ceiling — NST can be slow
            )

            if result.returncode != 0:
                logger.error("NST standalone scraper stderr:\n%s", result.stderr)
                raise RuntimeError(
                    f"NaturalStatTrick standalone scraper exited with code "
                    f"{result.returncode}."
                )

            raw = Path(tmp_path).read_text(encoding="utf-8")
            logger.info("NST standalone scraper completed successfully.")
            return raw

        finally:
            p = Path(tmp_path)
            if p.exists():
                p.unlink()

    # ----- content_key ------------------------------------------------------

    def content_key(self, raw: str) -> str:
        """Hash of stable data slice only (excludes timestamps and warnings).

        Hashing season + data tables ensures the StateManager fires a write
        when any actual stat values change, but not on timestamp-only changes.
        Uses MD5 — not a security hash, just a dedup key.
        """
        payload = json.loads(raw)
        stable = {
            "season": payload.get("season"),
            "data": payload.get("data", {}),
        }
        serialized = json.dumps(stable, sort_keys=True, separators=(",", ":"))
        return hashlib.md5(serialized.encode("utf-8")).hexdigest()

    # ----- parse ------------------------------------------------------------

    def parse(self, raw: str) -> list[dict]:
        """Parse standalone scraper JSON into batch envelopes.

        Fail-closed: if any critical section (team_5v5, team_all, player_5v5)
        did not succeed, raises ValueError to prevent writing partial snapshots.

        Returns:
          [{"_type": "team_splits", "records": [...]},
           {"_type": "player_splits", "records": [...]}]
        """
        payload = json.loads(raw)
        status = payload.get("status", {})

        critical_sections = ["team_5v5", "team_all", "player_5v5"]
        failed = [s for s in critical_sections if status.get(s) != "success"]
        if failed:
            raise ValueError(
                f"NST critical sections failed: {failed}. "
                "Aborting parse to prevent empty/partial GCS write."
            )

        # Store warnings for upsert() to include in payload envelope
        self._fetch_warnings: list[str] = payload.get("warnings", [])
        season: int = payload["season"]

        team_recs: list[dict] = []
        for split_key in ["team_5v5", "team_all"]:
            split_label = "5v5" if "5v5" in split_key else "all"
            for row in payload["data"].get(split_key, []):
                r = dict(row)
                team_raw = r.pop("team", "")
                team_recs.append({
                    "season": season,
                    "team": to_canonical(team_raw),
                    "split": split_label,
                    "gp": r.pop("gp", 0),
                    "toi": _mmss_to_float(r.pop("toi", r.pop("toi_per_gp", None))),
                    **r,
                })

        player_recs: list[dict] = []
        for row in payload["data"].get("player_5v5", []):
            r = dict(row)
            player_raw = r.pop("player", "")
            team_raw = r.pop("team", "")
            player_recs.append({
                "season": season,
                "team": to_canonical(team_raw),
                "player": player_raw,
                "player_name_norm": normalize_player_name(player_raw),
                "split": "5v5",
                "gp": r.pop("gp", 0),
                "toi": _mmss_to_float(r.pop("toi", r.pop("toi_per_gp", 0.0))),
                **r,
            })

        logger.info(
            "NST parse: %d team split records, %d player split records.",
            len(team_recs), len(player_recs),
        )
        return [
            {"_type": "team_splits", "records": team_recs},
            {"_type": "player_splits", "records": player_recs},
        ]

    # ----- validate ---------------------------------------------------------

    def validate(
        self, records: list[dict]
    ) -> list[Union[NhlTeamSplit, NhlPlayerSplit]]:
        """Route batch envelopes to Pydantic models."""
        validated: list[Union[NhlTeamSplit, NhlPlayerSplit]] = []
        for batch in records:
            model = NhlTeamSplit if batch["_type"] == "team_splits" else NhlPlayerSplit
            for r in batch["records"]:
                validated.append(model(**r))
        return validated

    # ----- upsert -----------------------------------------------------------

    def upsert(
        self, records: list[Union[NhlTeamSplit, NhlPlayerSplit]]
    ) -> None:
        """Write team splits and player splits to GCS with standard envelope."""
        storage = StorageManager(self.config["bucket"])
        ts = datetime.now(timezone.utc).isoformat()
        season_val = self.config["season"]
        warnings = getattr(self, "_fetch_warnings", [])

        # Standard envelope — aligns with repo schema_version: 1 convention
        base_envelope = {
            "schema_version": self.schema_version,
            "generated_at": ts,
            "scraper_key": self.source_key,
            "warnings": warnings,
            "updated": ts,          # legacy compatibility field
            "season": season_val,
        }

        teams = [r.model_dump() for r in records if isinstance(r, NhlTeamSplit)]
        players = [r.model_dump() for r in records if isinstance(r, NhlPlayerSplit)]

        if teams:
            team_payload = {
                **base_envelope,
                "record_count": len(teams),
                "team_splits": teams,
            }
            storage.persist_raw(source="nst_team_splits", data=team_payload)
            team_gcs = self.config.get("team_splits_gcs_object", "nhl/team_splits.json")
            storage.write_json(team_gcs, team_payload)
            logger.info("NST: wrote %d team split records to %s", len(teams), team_gcs)

        if players:
            player_payload = {
                **base_envelope,
                "record_count": len(players),
                "player_splits": players,
            }
            storage.persist_raw(source="nst_player_splits", data=player_payload)
            player_gcs = self.config.get("player_splits_gcs_object", "nhl/player_splits.json")
            storage.write_json(player_gcs, player_payload)
            logger.info(
                "NST: wrote %d player split records to %s", len(players), player_gcs
            )

        # nhl/nst_lines.json — reserved for future NST line report fetch.
        # Does NOT write to nhl/lines.json (owned by DailyFaceoffScraper).
        nst_lines_gcs = self.config.get("gcs_object", "nhl/nst_lines.json")
        logger.info(
            "NST: nst_lines path registered at %s (line fetch not yet implemented)",
            nst_lines_gcs,
        )
