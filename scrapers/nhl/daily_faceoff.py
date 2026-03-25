"""
scrapers/nhl/daily_faceoff.py

Daily Faceoff scraper.
Produces: nhl/goalies.json, nhl/lines.json

Review history:
  Round 1 (Gemini): initial implementation
  Round 2 (Claude): remove '_fallback' key from fallback parser (Bug 1);
                    NST path collision resolved (Bug 2)
  Round 3 (both): confirmed correct — parse_mode note documented
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.nhl.names import to_canonical, normalize_player_display, make_join_key

logger = logging.getLogger(__name__)

DAILY_FACEOFF_BASE = "https://www.dailyfaceoff.com"
GOALIES_URL = f"{DAILY_FACEOFF_BASE}/starting-goalies/"
DEFAULT_TIMEOUT = 20
RATE_LIMIT_DELAY = 1.5
MAX_RETRIES = 3
BACKOFF_BASE = 2.0
USER_AGENT = "sports-data-scraper/1.0 (github.com/jfmoon/sports-data-scraper)"

_STATUS_MAP: dict[str, str] = {
    "confirmed": "confirmed", "confirmed starter": "confirmed",
    "starting": "confirmed", "will start": "confirmed",
    "expected": "expected", "expected to start": "expected",
    "likely": "expected", "probable": "expected",
    "projected": "projected", "possible": "projected",
    "unconfirmed": "projected", "day-to-day": "projected",
    "tbd": "unknown", "unknown": "unknown", "": "unknown",
}

_TEAM_SLUGS: dict[str, str] = {
    "Anaheim Ducks": "anaheim-ducks", "Boston Bruins": "boston-bruins",
    "Buffalo Sabres": "buffalo-sabres", "Calgary Flames": "calgary-flames",
    "Carolina Hurricanes": "carolina-hurricanes", "Chicago Blackhawks": "chicago-blackhawks",
    "Colorado Avalanche": "colorado-avalanche", "Columbus Blue Jackets": "columbus-blue-jackets",
    "Dallas Stars": "dallas-stars", "Detroit Red Wings": "detroit-red-wings",
    "Edmonton Oilers": "edmonton-oilers", "Florida Panthers": "florida-panthers",
    "Los Angeles Kings": "los-angeles-kings", "Minnesota Wild": "minnesota-wild",
    "Montreal Canadiens": "montreal-canadiens", "Nashville Predators": "nashville-predators",
    "New Jersey Devils": "new-jersey-devils", "New York Islanders": "new-york-islanders",
    "New York Rangers": "new-york-rangers", "Ottawa Senators": "ottawa-senators",
    "Philadelphia Flyers": "philadelphia-flyers", "Pittsburgh Penguins": "pittsburgh-penguins",
    "San Jose Sharks": "san-jose-sharks", "Seattle Kraken": "seattle-kraken",
    "St. Louis Blues": "st-louis-blues", "Tampa Bay Lightning": "tampa-bay-lightning",
    "Toronto Maple Leafs": "toronto-maple-leafs", "Utah Hockey Club": "utah-hockey-club",
    "Vancouver Canucks": "vancouver-canucks", "Vegas Golden Knights": "vegas-golden-knights",
    "Washington Capitals": "washington-capitals", "Winnipeg Jets": "winnipeg-jets",
}


class NhlGoalieEntry(BaseModel):
    date: str
    team: str
    opponent: str | None = None
    home_away: str | None = None
    goalie_name: str
    starter_status_raw: str
    starter_status: str
    join_key: str | None = None
    source: str = "daily_faceoff"
    source_url: str = GOALIES_URL
    fetched_at: str = ""


class NhlLineEntry(BaseModel):
    date: str
    team: str
    line_type: str
    line_number: int | None = None
    player_1: str | None = None
    player_2: str | None = None
    player_3: str | None = None
    player_4: str | None = None
    scratches: list[str] = Field(default_factory=list)
    source: str = "daily_faceoff"
    source_url: str = ""
    fetched_at: str = ""


def normalize_goalie_status(raw: str) -> str:
    key = raw.strip().lower()
    if key in _STATUS_MAP:
        return _STATUS_MAP[key]
    if "confirm" in key:
        return "confirmed"
    if "start" in key or "will play" in key:
        return "confirmed"
    if "expect" in key or "likely" in key or "probable" in key:
        return "expected"
    if "project" in key or "possible" in key or "unconfirm" in key:
        return "projected"
    if key in ("tbd", "tbh", "n/a", ""):
        return "unknown"
    return "projected"


def _get_with_retry(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
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
                "DFO: request failed (attempt %d/%d), retry in %.1fs: %s — %s",
                attempt, MAX_RETRIES, wait, url, exc,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


class DailyFaceoffScraper(BaseScraper):

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers["User-Agent"] = USER_AGENT
        s.headers["Accept"] = "text/html,application/xhtml+xml"
        return s

    def _today(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _fetched_at(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def fetch(self) -> dict:
        session = self._session()
        logger.info("Daily Faceoff: fetching starting goalies from %s", GOALIES_URL)
        goalie_resp = _get_with_retry(session, GOALIES_URL)
        goalie_html = goalie_resp.text
        lines_html_by_team: dict[str, str] = {}
        if self.config.get("fetch_lines", True):
            for canonical_name, slug in _TEAM_SLUGS.items():
                url = f"{DAILY_FACEOFF_BASE}/teams/{slug}/line-combinations/"
                try:
                    time.sleep(RATE_LIMIT_DELAY)
                    resp = _get_with_retry(session, url)
                    lines_html_by_team[canonical_name] = resp.text
                    logger.debug("Daily Faceoff: fetched lines for %s", canonical_name)
                except requests.RequestException as exc:
                    logger.error("Daily Faceoff: failed to fetch lines for %s: %s", canonical_name, exc)
        return {
            "goalie_html": goalie_html,
            "goalie_url": GOALIES_URL,
            "lines_html_by_team": lines_html_by_team,
            "fetched_date": self._today(),
        }

    def content_key(self, raw: dict) -> Any:
        return raw.get("goalie_html", "")

    def _parse_goalies(self, html: str, date: str, fetched_at: str, source_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records: list[dict] = []
        matchup_headers = soup.select("span.text-3xl")
        if not matchup_headers:
            logger.warning(
                "Daily Faceoff: span.text-3xl not found — "
                "falling back to row-level search. Page structure may have changed."
            )
            return self._parse_goalies_fallback(soup, date, fetched_at, source_url)

        for header in matchup_headers:
            header_text = header.get_text(separator=" ", strip=True)
            if " at " not in header_text:
                continue
            away_raw, home_raw = [t.strip() for t in header_text.split(" at ", 1)]
            try:
                away_canonical = to_canonical(away_raw)
            except Exception:
                logger.warning("Daily Faceoff: unrecognized away team: %r", away_raw)
                away_canonical = away_raw
            try:
                home_canonical = to_canonical(home_raw)
            except Exception:
                logger.warning("Daily Faceoff: unrecognized home team: %r", home_raw)
                home_canonical = home_raw
            join_key = make_join_key(date, away_canonical, home_canonical)
            article = header.find_parent("article")
            if not article:
                continue
            goalie_cols = [
                d for d in article.find_all("div")
                if "w-1/2" in (d.get("class") or [])
            ]
            if len(goalie_cols) < 2:
                logger.warning(
                    "Daily Faceoff: expected 2 goalie columns for %s, got %d",
                    header_text, len(goalie_cols)
                )
                continue
            for col, (team, opponent, home_away) in zip(
                goalie_cols[:2],
                [
                    (away_canonical, home_canonical, "away"),
                    (home_canonical, away_canonical, "home"),
                ],
            ):
                name_el = col.select_one("span.text-lg")
                if name_el:
                    goalie_name = normalize_player_display(name_el.get_text(strip=True))
                else:
                    img = col.select_one("img[alt]")
                    goalie_name = normalize_player_display(img["alt"]) if img else ""
                if not goalie_name:
                    continue
                status_raw = ""
                status_container = col.select_one("div.font-bold")
                if status_container:
                    for span in status_container.find_all("span", recursive=True):
                        txt = span.get_text(strip=True)
                        if txt and not span.find("svg"):
                            status_raw = txt
                            break
                records.append({
                    "date": date, "team": team, "opponent": opponent,
                    "home_away": home_away, "goalie_name": goalie_name,
                    "starter_status_raw": status_raw,
                    "starter_status": normalize_goalie_status(status_raw),
                    "join_key": join_key,
                    "source": "daily_faceoff", "source_url": source_url, "fetched_at": fetched_at,
                })
        if not records:
            logger.warning("Daily Faceoff: goalie parse produced 0 records — falling back.")
            return self._parse_goalies_fallback(soup, date, fetched_at, source_url)
        return records

    def _parse_goalies_fallback(self, soup: BeautifulSoup, date: str, fetched_at: str, source_url: str) -> list[dict]:
        records = []
        body_text = soup.get_text(separator=" ", strip=True)[:2000]
        logger.warning("Daily Faceoff fallback parser active. Raw page snippet: %s", body_text)
        rows = soup.select("tr, li.goalie-row")
        for row in rows:
            cells = row.find_all(["td", "span"])
            if len(cells) < 2:
                continue
            goalie_name = normalize_player_display(cells[0].get_text(strip=True))
            status_raw_text = cells[1].get_text(strip=True)
            if not goalie_name:
                continue
            records.append({
                "date": date, "team": "", "opponent": None, "home_away": None,
                "goalie_name": goalie_name,
                "starter_status_raw": f"fallback_parse:{status_raw_text}",
                "starter_status": "unknown", "join_key": None,
                "source": "daily_faceoff", "source_url": source_url, "fetched_at": fetched_at,
            })
        return records

    def _parse_lines_for_team(self, team: str, html: str, date: str, fetched_at: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        records: list[dict] = []
        section = soup.find("section", {"id": "line_combos"})
        if not section:
            logger.warning(
                "Daily Faceoff lines: section#line_combos not found for %s — "
                "page structure may have changed", team
            )
            return []

        def _rows_under_header(header_id: str) -> list[list[str]]:
            header = section.find("span", {"id": header_id})
            if not header:
                return []
            try:
                container = header.parent.parent
            except AttributeError:
                return []
            if not container:
                return []
            result = []
            for div in container.find_all("div", recursive=True):
                classes = div.get("class") or []
                if all(c in classes for c in ["flex-row", "flex-wrap", "justify-evenly"]):
                    players = [
                        normalize_player_display(img["alt"])
                        for img in div.find_all("img", alt=True)
                        if img.get("alt", "").strip()
                    ]
                    if players:
                        result.append(players)
            return result

        for i, players in enumerate(_rows_under_header("forwards"), start=1):
            records.append({
                "date": date, "team": team, "line_type": "forward", "line_number": i,
                "player_1": players[0] if len(players) > 0 else None,
                "player_2": players[1] if len(players) > 1 else None,
                "player_3": players[2] if len(players) > 2 else None,
                "player_4": None, "scratches": [],
                "source": "daily_faceoff", "source_url": url, "fetched_at": fetched_at,
            })
        for i, players in enumerate(_rows_under_header("defense"), start=1):
            records.append({
                "date": date, "team": team, "line_type": "defense", "line_number": i,
                "player_1": players[0] if len(players) > 0 else None,
                "player_2": players[1] if len(players) > 1 else None,
                "player_3": None, "player_4": None, "scratches": [],
                "source": "daily_faceoff", "source_url": url, "fetched_at": fetched_at,
            })
        for i, players in enumerate(_rows_under_header("powerplay"), start=1):
            records.append({
                "date": date, "team": team, "line_type": "pp1", "line_number": i,
                "player_1": players[0] if len(players) > 0 else None,
                "player_2": players[1] if len(players) > 1 else None,
                "player_3": players[2] if len(players) > 2 else None,
                "player_4": players[3] if len(players) > 3 else None,
                "scratches": [],
                "source": "daily_faceoff", "source_url": url, "fetched_at": fetched_at,
            })
        scratches: list[str] = []
        injuries_header = section.find("span", string=lambda t: t and "Injuries" in t)
        if injuries_header:
            injuries_container = injuries_header.find_parent("div")
            if injuries_container:
                for img in injuries_container.find_all("img", alt=True):
                    name = normalize_player_display(img["alt"])
                    if name:
                        scratches.append(name)
        for r in records:
            r["scratches"] = scratches
        if not records:
            logger.warning(
                "Daily Faceoff: no line data parsed for %s — "
                "span#forwards or span#defense not found in section#line_combos", team
            )
        return records

    def parse(self, raw: dict) -> list[dict]:
        date = raw["fetched_date"]
        fetched_at = self._fetched_at()
        goalie_records = self._parse_goalies(
            html=raw["goalie_html"], date=date, fetched_at=fetched_at, source_url=raw["goalie_url"],
        )
        line_records: list[dict] = []
        for team, html in raw.get("lines_html_by_team", {}).items():
            url = f"{DAILY_FACEOFF_BASE}/teams/{_TEAM_SLUGS.get(team, '')}/line-combinations/"
            line_records.extend(self._parse_lines_for_team(team, html, date, fetched_at, url))
        return [
            {"_type": "goalies", "records": goalie_records},
            {"_type": "lines", "records": line_records},
        ]

    def validate(self, records: list[dict]) -> list[BaseModel]:
        validated: list[BaseModel] = []
        for batch in records:
            if batch["_type"] == "goalies":
                for r in batch["records"]:
                    validated.append(NhlGoalieEntry(**r))
            elif batch["_type"] == "lines":
                for r in batch["records"]:
                    validated.append(NhlLineEntry(**r))
        return validated

    def upsert(self, records: list[BaseModel]) -> None:
        storage = StorageManager(self.config["bucket"])
        fetched_at = self._fetched_at()

        goalies = [r.model_dump() for r in records if isinstance(r, NhlGoalieEntry)]
        lines = [r.model_dump() for r in records if isinstance(r, NhlLineEntry)]

        goalie_payload = {
            # Standard envelope — schema_version 1
            "schema_version": 1,
            "generated_at": fetched_at,
            "scraper_key": "daily_faceoff",
            "record_count": len(goalies),
            "warnings": [],  # TODO: propagate scraper warnings here
            # Existing fields — unchanged
            "updated": fetched_at,
            "date": goalies[0]["date"] if goalies else "",
            "goalie_count": len(goalies),
            "goalies": goalies,
        }
        storage.persist_raw(source="daily_faceoff_goalies", data=goalie_payload)
        storage.write_json(self.config["gcs_object"], goalie_payload)
        logger.info(
            "Daily Faceoff: wrote %d goalie records to %s",
            len(goalies), self.config["gcs_object"],
        )

        lines_gcs = self.config.get("lines_gcs_object", "nhl/lines.json")
        if lines:
            lines_payload = {
                # Standard envelope — schema_version 1
                "schema_version": 1,
                "generated_at": fetched_at,
                "scraper_key": "daily_faceoff",
                "record_count": len(lines),
                "warnings": [],  # TODO: propagate scraper warnings here
                # Existing fields — unchanged
                "updated": fetched_at,
                "date": lines[0]["date"] if lines else "",
                "entry_count": len(lines),
                "lines": lines,
            }
            storage.persist_raw(source="daily_faceoff_lines", data=lines_payload)
            storage.write_json(lines_gcs, lines_payload)
            logger.info(
                "Daily Faceoff: wrote %d line records to %s", len(lines), lines_gcs
            )
