"""
scrapers/mlb/weather.py

Fetches game-day weather for each MLB ballpark.
Uses the Open-Meteo API (free, no API key required) as the default provider.
Provider is abstracted behind WeatherProvider so it can be swapped.

Dome/retractable-roof stadiums are explicitly marked — no weather data fetched
for those games, and the output makes the dome status explicit.

Weather is matched to games via mlb/probables.json (read from GCS or
fetched live if not available). Requires GCS read permission if using live data.

Output: mlb/weather.json
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import requests
from pydantic import BaseModel

from base.scraper import BaseScraper
from base.storage import StorageManager
from scrapers.mlb.names import to_canonical

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stadium metadata
# Loaded from data/crosswalks/mlb_teams.json at runtime.
# ---------------------------------------------------------------------------

_STADIUM_META_CACHE: Optional[dict] = None

def _load_stadium_meta() -> dict:
    global _STADIUM_META_CACHE
    if _STADIUM_META_CACHE is None:
        crosswalk_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "crosswalks", "mlb_teams.json"
        )
        with open(crosswalk_path) as f:
            data = json.load(f)
        _STADIUM_META_CACHE = data.get("stadium_metadata", {})
    return _STADIUM_META_CACHE


SOURCE = "open_meteo"

# ---------------------------------------------------------------------------
# Weather provider abstraction
# ---------------------------------------------------------------------------

class WeatherProvider:
    """
    Abstract base — swap by setting MLB_WEATHER_PROVIDER env var.
    Currently only Open-Meteo is implemented (no API key needed).
    Add a new provider by subclassing and registering in PROVIDERS.
    """

    def fetch_forecast(self, lat: float, lon: float, date: str) -> dict:
        raise NotImplementedError


class OpenMeteoProvider(WeatherProvider):
    """
    Fetches hourly forecast from Open-Meteo for a given lat/lon/date.
    Returns the first matching hour's data (game time approximate).
    No API key required.
    """
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def fetch_forecast(self, lat: float, lon: float, date: str) -> dict:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation_probability,precipitation,windspeed_10m,winddirection_10m,relativehumidity_2m,weathercode",
            "temperature_unit": "fahrenheit",
            "windspeed_unit": "mph",
            "timezone": "auto",
            "start_date": date,
            "end_date": date,
        }
        resp = requests.get(self.BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def extract_game_hour(self, forecast: dict, game_hour: int = 19) -> dict:
        """
        Extract weather at a specific hour (default 7pm local = typical first pitch).
        Falls back to noon if hour not found.
        """
        hourly = forecast.get("hourly", {})
        times = hourly.get("time", [])

        # Find index for target hour.
        target_idx = None
        for i, t in enumerate(times):
            try:
                h = int(t.split("T")[1].split(":")[0])
                if h == game_hour:
                    target_idx = i
                    break
            except (IndexError, ValueError):
                continue

        if target_idx is None:
            # Fallback to noon.
            for i, t in enumerate(times):
                try:
                    h = int(t.split("T")[1].split(":")[0])
                    if h == 12:
                        target_idx = i
                        break
                except (IndexError, ValueError):
                    continue

        if target_idx is None:
            target_idx = 0

        def _get(key):
            vals = hourly.get(key, [])
            return vals[target_idx] if target_idx < len(vals) else None

        wcode = _get("weathercode")
        return {
            "temperature_f": _get("temperature_2m"),
            "wind_mph": _get("windspeed_10m"),
            "wind_direction_deg": _get("winddirection_10m"),
            "precip_pct": _get("precipitation_probability"),
            "humidity_pct": _get("relativehumidity_2m"),
            "conditions": _weathercode_to_label(wcode),
        }


def _weathercode_to_label(code) -> Optional[str]:
    """Map WMO weather interpretation code to human-readable label."""
    if code is None:
        return None
    code = int(code)
    mapping = {
        0: "Clear",
        1: "Mostly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Fog", 48: "Icy Fog",
        51: "Light Drizzle", 53: "Drizzle", 55: "Heavy Drizzle",
        61: "Light Rain", 63: "Rain", 65: "Heavy Rain",
        71: "Light Snow", 73: "Snow", 75: "Heavy Snow",
        80: "Showers", 81: "Showers", 82: "Heavy Showers",
        95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm",
    }
    return mapping.get(code, f"Code {code}")


def _degrees_to_direction(degrees) -> Optional[str]:
    """Convert wind direction degrees to compass label."""
    if degrees is None:
        return None
    try:
        deg = float(degrees)
    except (TypeError, ValueError):
        return None
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = round(deg / 45) % 8
    return dirs[idx]


PROVIDERS: dict[str, WeatherProvider] = {
    "open_meteo": OpenMeteoProvider(),
}

# Stadium coordinates — lat/lon for weather fetch.
# These are ballpark coordinates, not city centroids.
STADIUM_COORDS: dict[str, tuple[float, float]] = {
    "Arizona Diamondbacks":  (33.4453, -112.0667),
    "Atlanta Braves":        (33.8908, -84.4680),
    "Baltimore Orioles":     (39.2838, -76.6216),
    "Boston Red Sox":        (42.3467, -71.0972),
    "Chicago Cubs":          (41.9484, -87.6553),
    "Chicago White Sox":     (41.8299, -87.6338),
    "Cincinnati Reds":       (39.0979, -84.5079),
    "Cleveland Guardians":   (41.4962, -81.6852),
    "Colorado Rockies":      (39.7559, -104.9942),
    "Detroit Tigers":        (42.3390, -83.0485),
    "Houston Astros":        (29.7573, -95.3555),
    "Kansas City Royals":    (39.0517, -94.4803),
    "Los Angeles Angels":    (33.8003, -117.8827),
    "Los Angeles Dodgers":   (34.0739, -118.2400),
    "Miami Marlins":         (25.7781, -80.2197),
    "Milwaukee Brewers":     (43.0280, -87.9712),
    "Minnesota Twins":       (44.9817, -93.2783),
    "New York Mets":         (40.7571, -73.8458),
    "New York Yankees":      (40.8296, -73.9262),
    "Oakland Athletics":     (38.5803, -121.5029),  # Sacramento (Sutter Health Park)
    "Philadelphia Phillies": (39.9061, -75.1665),
    "Pittsburgh Pirates":    (40.4469, -80.0057),
    "San Diego Padres":      (32.7073, -117.1566),
    "San Francisco Giants":  (37.7786, -122.3893),
    "Seattle Mariners":      (47.5914, -122.3325),
    "St. Louis Cardinals":   (38.6226, -90.1928),
    "Tampa Bay Rays":        (27.7683, -82.6534),
    "Texas Rangers":         (32.7473, -97.0827),
    "Toronto Blue Jays":     (43.6414, -79.3894),
    "Washington Nationals":  (38.8730, -77.0074),
}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class WeatherRecord(BaseModel):
    game_id: str
    date: str
    away_team: str
    home_team: str
    stadium: str
    city: str
    state: str
    is_dome: bool
    is_retractable: bool
    temperature_f: Optional[float]
    wind_mph: Optional[float]
    wind_direction: Optional[str]
    precip_pct: Optional[float]
    humidity_pct: Optional[float]
    conditions: Optional[str]
    source: str
    fetched_at: str


class WeatherSnapshot(BaseModel):
    updated: str
    game_count: int
    games: list[WeatherRecord]


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class WeatherScraper(BaseScraper):
    """
    Fetches weather for each scheduled MLB game using ballpark coordinates.

    Game list is sourced from the MLB Stats API schedule (same as probables).
    Weather is only fetched for outdoor stadiums — domes get null weather fields
    with is_dome=True.

    Provider: Open-Meteo by default (no API key).
    Override via MLB_WEATHER_PROVIDER env var if you add additional providers to PROVIDERS dict.
    """

    def _get_season(self) -> int:
        return int(self.config.get("season", datetime.now(timezone.utc).year))

    def _get_provider(self) -> WeatherProvider:
        provider_name = os.environ.get("MLB_WEATHER_PROVIDER", "open_meteo")
        if provider_name not in PROVIDERS:
            raise RuntimeError(
                f"Unknown MLB_WEATHER_PROVIDER='{provider_name}'. "
                f"Available: {list(PROVIDERS.keys())}"
            )
        return PROVIDERS[provider_name]

    def fetch(self) -> dict:
        """
        1. Fetch today's MLB schedule from Stats API.
        2. For each game, fetch weather for the home team's stadium.
        """
        from datetime import timedelta

        today = datetime.now(timezone.utc).date()
        end_date = today + timedelta(days=int(self.config.get("days_ahead", 2)))

        # Fetch schedule.
        schedule_params = {
            "sportId": 1,
            "hydrate": "team,linescore",
            "fields": "dates,date,games,gamePk,gameDate,status,teams,away,home,team,name",
            "startDate": today.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
        }
        logger.info("Fetching MLB schedule for weather %s → %s", today, end_date)
        sched_resp = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params=schedule_params,
            timeout=15,
        )
        sched_resp.raise_for_status()
        schedule = sched_resp.json()

        stadium_meta = _load_stadium_meta()
        provider = self._get_provider()
        game_weather = []

        for date_entry in schedule.get("dates", []):
            date_str = date_entry.get("date", "")
            for game in date_entry.get("games", []):
                game_pk = str(game.get("gamePk", ""))
                game_date = game.get("gameDate", "")
                teams = game.get("teams", {})

                away_name = to_canonical(
                    teams.get("away", {}).get("team", {}).get("name", "")
                )
                home_name = to_canonical(
                    teams.get("home", {}).get("team", {}).get("name", "")
                )

                meta = stadium_meta.get(home_name, {})
                is_dome = meta.get("is_dome", False)
                is_retractable = meta.get("is_retractable", False)
                stadium = meta.get("stadium", "")
                city = meta.get("city", "")
                state = meta.get("state", "")

                if is_dome:
                    # Permanent dome (Tropicana Field) — weather not applicable.
                    # Retractable-roof stadiums are NOT treated as domes here;
                    # weather is fetched for them and the Transform layer decides
                    # whether to apply it based on likely roof status.
                    game_weather.append({
                        "game_id": game_pk,
                        "date": date_str,
                        "away_team": away_name,
                        "home_team": home_name,
                        "stadium": stadium,
                        "city": city,
                        "state": state,
                        "is_dome": True,
                        "is_retractable": False,
                        "weather_raw": None,
                    })
                    continue

                coords = STADIUM_COORDS.get(home_name)
                if not coords:
                    logger.warning(
                        "No stadium coords for team '%s' — skipping weather", home_name
                    )
                    game_weather.append({
                        "game_id": game_pk,
                        "date": date_str,
                        "away_team": away_name,
                        "home_team": home_name,
                        "stadium": stadium,
                        "city": city,
                        "state": state,
                        "is_dome": False,
                        "is_retractable": is_retractable,
                        "weather_raw": None,
                    })
                    continue

                try:
                    lat, lon = coords
                    forecast = provider.fetch_forecast(lat, lon, date_str)
                    # Extract for typical first-pitch hour; configurable.
                    game_hour = int(self.config.get("game_hour", 19))
                    if isinstance(provider, OpenMeteoProvider):
                        weather = provider.extract_game_hour(forecast, game_hour)
                    else:
                        weather = forecast  # custom providers return pre-extracted dict

                    game_weather.append({
                        "game_id": game_pk,
                        "date": date_str,
                        "away_team": away_name,
                        "home_team": home_name,
                        "stadium": stadium,
                        "city": city,
                        "state": state,
                        "is_dome": False,
                        "is_retractable": is_retractable,
                        "weather_raw": weather,
                    })
                    logger.debug("Fetched weather for %s @ %s", away_name, home_name)
                except Exception as e:
                    logger.warning(
                        "Weather fetch failed for %s @ %s: %s — using nulls",
                        away_name, home_name, e
                    )
                    game_weather.append({
                        "game_id": game_pk,
                        "date": date_str,
                        "away_team": away_name,
                        "home_team": home_name,
                        "stadium": stadium,
                        "city": city,
                        "state": state,
                        "is_dome": False,
                        "is_retractable": is_retractable,
                        "weather_raw": None,
                    })

        return {"games": game_weather}

    def content_key(self, raw: dict) -> str:
        """Hash on game IDs + temperature — ignores fetched_at."""
        parts = []
        for g in raw.get("games", []):
            w = g.get("weather_raw") or {}
            parts.append(f"{g['game_id']}:{w.get('temperature_f')}:{w.get('wind_mph')}")
        return "|".join(sorted(parts))

    def parse(self, raw: dict) -> list[dict]:
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = []

        for g in raw.get("games", []):
            w = g.get("weather_raw") or {}
            wind_dir_deg = w.get("wind_direction_deg")
            wind_dir = _degrees_to_direction(wind_dir_deg)

            records.append({
                "game_id": g["game_id"],
                "date": g["date"],
                "away_team": g["away_team"],
                "home_team": g["home_team"],
                "stadium": g.get("stadium", ""),
                "city": g.get("city", ""),
                "state": g.get("state", ""),
                "is_dome": g.get("is_dome", False),
                "is_retractable": g.get("is_retractable", False),
                "temperature_f": w.get("temperature_f") if not g.get("is_dome") else None,
                "wind_mph": w.get("wind_mph") if not g.get("is_dome") else None,
                "wind_direction": wind_dir if not g.get("is_dome") else None,
                "precip_pct": w.get("precip_pct") if not g.get("is_dome") else None,
                "humidity_pct": w.get("humidity_pct") if not g.get("is_dome") else None,
                "conditions": w.get("conditions") if not g.get("is_dome") else "Indoor",
                "source": SOURCE,
                "fetched_at": fetched_at,
            })

        logger.info("Parsed %d game weather records", len(records))
        return records

    def validate(self, records: list[dict]) -> list[WeatherRecord]:
        validated = []
        for r in records:
            try:
                validated.append(WeatherRecord(**r))
            except Exception as e:
                logger.warning("Invalid weather record: %s | game=%s", e, r.get("game_id"))
        return validated

    def upsert(self, validated: list[WeatherRecord]) -> None:
        sm = StorageManager(self.config["bucket"])
        fetched_at = datetime.now(timezone.utc).isoformat()

        payload = WeatherSnapshot(
            updated=fetched_at,
            game_count=len(validated),
            games=validated,
        ).model_dump(mode="json")

        sm.persist_raw(source="mlb_weather", data=payload)
        sm.write_json(blob_name=self.config["gcs_object"], data=payload)
        logger.info("Wrote mlb/weather.json (%d games)", len(validated))
