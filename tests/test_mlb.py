"""
tests/test_mlb.py

Unit tests for MLB scraper parsing and normalization logic.

Updated after code review:
- TestStadiumMapping: corrected dome vs retractable assertions (only Tampa Bay is_dome=True)
- TestLineupContentKey: new — verifies mid-order swaps and single substitutions are detected
- TestStatcastListContract: new — verifies statcast scrapers return list, not dict
- TestFangraphsTeamsPartialFailure: new — verifies one failed split doesn't kill the others
- TestWeatherPartialFailure: new — verifies one stadium failure doesn't block the rest
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Team name canonicalization
# ---------------------------------------------------------------------------
from scrapers.mlb.names import to_canonical, CANONICAL_TEAMS


class TestToCanonical:
    def test_already_canonical_passthrough(self):
        for name in CANONICAL_TEAMS:
            assert to_canonical(name) == name

    def test_abbreviation_resolution(self):
        assert to_canonical("NYY") == "New York Yankees"
        assert to_canonical("nyy") == "New York Yankees"
        assert to_canonical("LAD") == "Los Angeles Dodgers"
        assert to_canonical("lad") == "Los Angeles Dodgers"
        assert to_canonical("STL") == "St. Louis Cardinals"

    def test_nickname_only(self):
        assert to_canonical("Yankees") == "New York Yankees"
        assert to_canonical("Red Sox") == "Boston Red Sox"
        assert to_canonical("D-backs") == "Arizona Diamondbacks"
        assert to_canonical("Athletics") == "Oakland Athletics"
        assert to_canonical("A's") == "Oakland Athletics"

    def test_legacy_name(self):
        assert to_canonical("Indians") == "Cleveland Guardians"

    def test_case_insensitive(self):
        assert to_canonical("DODGERS") == "Los Angeles Dodgers"
        assert to_canonical("dodgers") == "Los Angeles Dodgers"

    def test_fangraphs_variants(self):
        assert to_canonical("LA Dodgers") == "Los Angeles Dodgers"
        assert to_canonical("LA Angels") == "Los Angeles Angels"

    def test_ambiguous_raises(self):
        with pytest.raises(ValueError, match="ambiguous"):
            to_canonical("New York")
        with pytest.raises(ValueError, match="ambiguous"):
            to_canonical("Chicago")

    def test_unknown_strict_raises(self):
        with pytest.raises(ValueError, match="not found in canonical map"):
            to_canonical("Nonexistent FC", strict=True)

    def test_unknown_nonstrict_returns_original(self):
        result = to_canonical("Unknown Team", strict=False)
        assert result == "Unknown Team"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            to_canonical("")


# ---------------------------------------------------------------------------
# Probable pitcher handedness parsing
# ---------------------------------------------------------------------------
from scrapers.mlb.probables import _parse_pitcher, ProbablePitcherRecord


class TestParsePitcher:
    def test_none_returns_empty(self):
        name, hand, pid, confirmed = _parse_pitcher(None)
        assert name is None
        assert hand is None
        assert pid is None
        assert confirmed is False

    def test_empty_dict_returns_empty(self):
        name, hand, pid, confirmed = _parse_pitcher({})
        assert confirmed is False

    def test_full_pitcher(self):
        d = {"fullName": "Gerrit Cole", "id": 543037, "pitchHand": {"code": "R"}}
        name, hand, pid, confirmed = _parse_pitcher(d)
        assert name == "Gerrit Cole"
        assert hand == "R"
        assert pid == 543037
        assert confirmed is True

    def test_left_hand(self):
        d = {"fullName": "Clayton Kershaw", "id": 477132, "pitchHand": {"code": "L"}}
        _, hand, _, _ = _parse_pitcher(d)
        assert hand == "L"

    def test_string_hand(self):
        d = {"fullName": "Some Pitcher", "id": 1, "pitchHand": "R"}
        _, hand, _, _ = _parse_pitcher(d)
        assert hand == "R"

    def test_invalid_hand_normalizes_to_none(self):
        rec = ProbablePitcherRecord(
            game_id="1", date="2026-04-01", commence_time="2026-04-01T19:05:00Z",
            away_team="New York Yankees", home_team="Boston Red Sox",
            away_pitcher="Test Pitcher", home_pitcher=None,
            away_hand="X", home_hand=None,
            away_pitcher_id=None, home_pitcher_id=None,
            away_confirmed=True, home_confirmed=False,
            source="test", fetched_at=datetime.now(timezone.utc).isoformat(),
        )
        assert rec.away_hand is None

    def test_switch_pitcher_accepted(self):
        rec = ProbablePitcherRecord(
            game_id="2", date="2026-04-01", commence_time="2026-04-01T19:05:00Z",
            away_team="New York Yankees", home_team="Boston Red Sox",
            away_pitcher="Pat Venditte", home_pitcher=None,
            away_hand="S", home_hand=None,
            away_pitcher_id=None, home_pitcher_id=None,
            away_confirmed=True, home_confirmed=False,
            source="test", fetched_at=datetime.now(timezone.utc).isoformat(),
        )
        assert rec.away_hand == "S"


# ---------------------------------------------------------------------------
# Split normalization
# ---------------------------------------------------------------------------
from scrapers.mlb.fangraphs_teams import SPLITS


class TestSplitNormalization:
    def test_expected_splits_present(self):
        assert "overall" in SPLITS
        assert "vs_lhp" in SPLITS
        assert "vs_rhp" in SPLITS

    def test_overall_has_no_filter(self):
        assert SPLITS["overall"] is None

    def test_lhp_rhp_have_filter_values(self):
        assert SPLITS["vs_lhp"] is not None
        assert SPLITS["vs_rhp"] is not None

    def test_all_split_values_are_strings_or_none(self):
        for k, v in SPLITS.items():
            assert v is None or isinstance(v, str), f"Split {k} has invalid value {v!r}"


# ---------------------------------------------------------------------------
# Statcast numeric parsing
# ---------------------------------------------------------------------------
from scrapers.mlb.statcast_pitchers import _float as sc_float, _int as sc_int


class TestStatcastNumericParsing:
    def test_normal_float(self):
        assert sc_float("0.312") == pytest.approx(0.312)
        assert sc_float(".312") == pytest.approx(0.312)

    def test_integer_string(self):
        assert sc_float("2026") == pytest.approx(2026.0)

    def test_null_variants(self):
        for v in (None, "", "null", "None", ".", "-."):
            assert sc_float(v) is None, f"Expected None for {v!r}"

    def test_int_conversion(self):
        assert sc_int("450") == 450
        assert sc_int("450.7") == 450
        assert sc_int(None) is None
        assert sc_int("") is None

    def test_percentage_string(self):
        assert sc_float("32.5") == pytest.approx(32.5)


# ---------------------------------------------------------------------------
# NEW: Statcast list contract
# Verifies statcast_pitchers and statcast_hitters return list[dict] from
# parse() and list[BaseModel] from validate() — the contract ScraperRunner
# depends on. The old statcast.py returned dict and crashed the runner.
# ---------------------------------------------------------------------------
from scrapers.mlb.statcast_pitchers import StatcastPitchersScraper
from scrapers.mlb.statcast_hitters import StatcastHittersScraper


class TestStatcastListContract:
    def _make_minimal_row(self) -> dict:
        return {
            "player_id": "123456",
            "last_name, first_name": "Cole, Gerrit",
            "team_name_abb": "NYY",
            "pa": "100",
            "est_woba": "0.310",
            "est_ba": "0.250",
            "est_slg": "0.420",
            "p_era": "3.20",
            "whiff_percent": "28.5",
            "k_percent": "27.0",
            "bb_percent": "6.5",
            "barrel_batted_rate": "7.2",
            "hard_hit_percent": "38.1",
            "exit_velocity_avg": "89.2",
        }

    def test_pitcher_parse_returns_list(self):
        scraper = StatcastPitchersScraper.__new__(StatcastPitchersScraper)
        scraper.config = {"season": 2026, "min_pa": 20}
        raw = {"season": 2026, "rows": [self._make_minimal_row()]}
        result = scraper.parse(raw)
        assert isinstance(result, list), f"parse() must return list, got {type(result)}"

    def test_pitcher_validate_returns_list(self):
        scraper = StatcastPitchersScraper.__new__(StatcastPitchersScraper)
        scraper.config = {"season": 2026, "min_pa": 20}
        raw = {"season": 2026, "rows": [self._make_minimal_row()]}
        records = scraper.parse(raw)
        validated = scraper.validate(records)
        assert isinstance(validated, list), f"validate() must return list, got {type(validated)}"
        assert len(validated) == 1

    def test_hitter_parse_returns_list(self):
        scraper = StatcastHittersScraper.__new__(StatcastHittersScraper)
        scraper.config = {"season": 2026, "min_pa": 20}
        raw = {"season": 2026, "rows": [self._make_minimal_row()]}
        result = scraper.parse(raw)
        assert isinstance(result, list)

    def test_hitter_validate_returns_list(self):
        scraper = StatcastHittersScraper.__new__(StatcastHittersScraper)
        scraper.config = {"season": 2026, "min_pa": 20}
        raw = {"season": 2026, "rows": [self._make_minimal_row()]}
        records = scraper.parse(raw)
        validated = scraper.validate(records)
        assert isinstance(validated, list)
        assert len(validated) == 1


# ---------------------------------------------------------------------------
# Stadium mapping and dome/retractable classification
# ---------------------------------------------------------------------------
from scrapers.mlb.weather import _load_stadium_meta, STADIUM_COORDS, _degrees_to_direction


class TestStadiumMapping:
    def test_all_canonical_teams_have_coords(self):
        missing = [team for team in CANONICAL_TEAMS if team not in STADIUM_COORDS]
        assert missing == [], f"Missing coords for: {missing}"

    def test_only_tropicana_is_permanent_dome(self):
        """
        is_dome=True must only be set for permanently enclosed stadiums.
        As of 2026, only Tropicana Field qualifies. Retractable roofs are
        is_retractable=True, not is_dome=True — weather is always fetched for them.
        """
        meta = _load_stadium_meta()
        dome_teams = [team for team, m in meta.items() if m.get("is_dome")]
        assert dome_teams == ["Tampa Bay Rays"], (
            f"Expected only Tampa Bay Rays as is_dome=True, got: {dome_teams}"
        )

    def test_retractable_roof_stadiums_correctly_classified(self):
        meta = _load_stadium_meta()
        expected_retractable = {
            "Arizona Diamondbacks",   # Chase Field
            "Houston Astros",          # Minute Maid Park
            "Miami Marlins",           # loanDepot park
            "Milwaukee Brewers",       # American Family Field
            "Seattle Mariners",        # T-Mobile Park
            "Texas Rangers",           # Globe Life Field
            "Toronto Blue Jays",       # Rogers Centre
        }
        for team in expected_retractable:
            assert meta[team]["is_retractable"] is True, f"{team} should be is_retractable=True"
            assert meta[team]["is_dome"] is False, (
                f"{team} is retractable, not a permanent dome — is_dome must be False"
            )

    def test_outdoor_stadiums_have_no_roof_flags(self):
        meta = _load_stadium_meta()
        for team in ["New York Yankees", "Boston Red Sox", "Los Angeles Dodgers", "Colorado Rockies"]:
            assert meta[team]["is_dome"] is False
            assert meta[team]["is_retractable"] is False

    def test_wind_direction_cardinal(self):
        assert _degrees_to_direction(0) == "N"
        assert _degrees_to_direction(90) == "E"
        assert _degrees_to_direction(180) == "S"
        assert _degrees_to_direction(270) == "W"
        assert _degrees_to_direction(45) == "NE"
        assert _degrees_to_direction(None) is None


# ---------------------------------------------------------------------------
# Lineup batting order parsing
# ---------------------------------------------------------------------------
from scrapers.mlb.lineups import _parse_lineup_side, LineupsScraper


class TestLineupParsing:
    def test_empty_side_returns_empty(self):
        slots, confirmed = _parse_lineup_side({})
        assert slots == []
        assert confirmed is False

    def test_confirmed_lineup_parsed_correctly(self):
        players = {
            "ID1001": {
                "battingOrder": "100",
                "person": {"fullName": "Aaron Judge", "id": 1001},
                "position": {"abbreviation": "RF"},
                "batSide": {"code": "R"},
            },
            "ID1002": {
                "battingOrder": "200",
                "person": {"fullName": "Juan Soto", "id": 1002},
                "position": {"abbreviation": "LF"},
                "batSide": {"code": "L"},
            },
        }
        slots, confirmed = _parse_lineup_side({"players": players})
        assert confirmed is True
        assert len(slots) == 2
        names = [s["player_name"] for s in slots]
        assert "Aaron Judge" in names
        assert "Juan Soto" in names

    def test_batting_order_sorted(self):
        players = {
            "ID2001": {"battingOrder": "900", "person": {"fullName": "P9", "id": 9},  "position": {"abbreviation": "P"},  "batSide": {}},
            "ID2002": {"battingOrder": "100", "person": {"fullName": "P1", "id": 1},  "position": {"abbreviation": "SS"}, "batSide": {}},
            "ID2003": {"battingOrder": "500", "person": {"fullName": "P5", "id": 5},  "position": {"abbreviation": "CF"}, "batSide": {}},
        }
        slots, _ = _parse_lineup_side({"players": players})
        orders = [s["batting_order"] for s in slots]
        assert orders == sorted(orders)

    def test_substitute_entries_excluded(self):
        players = {
            "ID3001": {"battingOrder": "100", "person": {"fullName": "Starter", "id": 1}, "position": {"abbreviation": "1B"}, "batSide": {}},
            "ID3002": {"battingOrder": "101", "person": {"fullName": "Sub",     "id": 2}, "position": {"abbreviation": "PH"}, "batSide": {}},
        }
        slots, _ = _parse_lineup_side({"players": players})
        assert len(slots) == 1
        assert slots[0]["player_name"] == "Starter"

    def test_no_batting_order_excluded(self):
        players = {
            "ID4001": {"person": {"fullName": "Bench Player", "id": 99}, "position": {"abbreviation": "C"}, "batSide": {}},
        }
        slots, confirmed = _parse_lineup_side({"players": players})
        assert slots == []
        assert confirmed is False


# ---------------------------------------------------------------------------
# NEW: Lineup content_key detects real lineup changes
# The previous proxy (first player ID alphabetically) missed mid-order swaps
# and single-player substitutions anywhere but position 1.
# ---------------------------------------------------------------------------

def _make_raw_game(game_pk: str, away_players: dict, home_players: dict) -> dict:
    """Helper to build a raw game dict in the shape LineupsScraper.content_key() expects."""
    return {
        "game_pk": game_pk,
        "date": "2026-04-01",
        "commence_time": "2026-04-01T19:05:00Z",
        "feed": {
            "liveData": {
                "boxscore": {
                    "teams": {
                        "away": {"players": away_players},
                        "home": {"players": home_players},
                    }
                }
            }
        },
    }


def _make_player(batting_order: int, player_id: int) -> dict:
    return {
        "battingOrder": str(batting_order * 100),
        "person": {"fullName": f"Player{player_id}", "id": player_id},
        "position": {"abbreviation": "OF"},
        "batSide": {"code": "R"},
    }


class TestLineupContentKey:
    def _scraper(self):
        s = LineupsScraper.__new__(LineupsScraper)
        s.config = {"days_ahead": 1, "bucket": "test-bucket", "gcs_object": "mlb/lineups.json"}
        return s

    def _base_lineup(self) -> dict:
        """9-player lineup: player IDs 101-109 in batting positions 1-9."""
        return {f"ID{100+i}": _make_player(i, 100+i) for i in range(1, 10)}

    def test_identical_lineups_same_key(self):
        lineup = self._base_lineup()
        raw1 = {"games": [_make_raw_game("777", lineup, lineup)]}
        raw2 = {"games": [_make_raw_game("777", lineup, lineup)]}
        assert self._scraper().content_key(raw1) == self._scraper().content_key(raw2)

    def test_single_player_swap_detected(self):
        """Replace the 9th hitter (player 109) with player 200. Must produce different key."""
        lineup_a = self._base_lineup()
        lineup_b = self._base_lineup()
        lineup_b["ID109"] = _make_player(9, 200)   # swap 9th hitter

        raw_a = {"games": [_make_raw_game("777", lineup_a, lineup_a)]}
        raw_b = {"games": [_make_raw_game("777", lineup_b, lineup_b)]}
        assert self._scraper().content_key(raw_a) != self._scraper().content_key(raw_b)

    def test_mid_order_swap_detected(self):
        """Swap players at positions 4 and 5. Must produce different key."""
        lineup_a = self._base_lineup()
        lineup_b = self._base_lineup()
        # Exchange IDs at batting positions 4 and 5
        lineup_b["ID104"] = _make_player(4, 105)
        lineup_b["ID105"] = _make_player(5, 104)

        raw_a = {"games": [_make_raw_game("777", lineup_a, lineup_a)]}
        raw_b = {"games": [_make_raw_game("777", lineup_b, lineup_b)]}
        assert self._scraper().content_key(raw_a) != self._scraper().content_key(raw_b)

    def test_home_lineup_change_detected(self):
        """Away unchanged, home changes 3rd hitter. Must produce different key."""
        away = self._base_lineup()
        home_a = self._base_lineup()
        home_b = self._base_lineup()
        home_b["ID103"] = _make_player(3, 999)   # swap 3rd hitter on home side

        raw_a = {"games": [_make_raw_game("777", away, home_a)]}
        raw_b = {"games": [_make_raw_game("777", away, home_b)]}
        assert self._scraper().content_key(raw_a) != self._scraper().content_key(raw_b)

    def test_sub_entries_dont_affect_key(self):
        """
        Adding a substitute (battingOrder 401 = mid-inning sub for spot 4) must
        NOT change the content_key — subs should be invisible to dedup.
        """
        lineup_a = self._base_lineup()
        lineup_b = self._base_lineup()
        lineup_b["ID_SUB"] = {
            "battingOrder": "401",   # sub entry, order % 100 != 0
            "person": {"fullName": "PinchHitter", "id": 9999},
            "position": {"abbreviation": "PH"},
            "batSide": {"code": "L"},
        }
        raw_a = {"games": [_make_raw_game("777", lineup_a, lineup_a)]}
        raw_b = {"games": [_make_raw_game("777", lineup_b, lineup_b)]}
        assert self._scraper().content_key(raw_a) == self._scraper().content_key(raw_b)

    def test_empty_lineup_produces_stable_key(self):
        """Games with no lineup yet must not crash and must produce a stable empty key."""
        raw = {"games": [_make_raw_game("777", {}, {})]}
        key1 = self._scraper().content_key(raw)
        key2 = self._scraper().content_key(raw)
        assert key1 == key2
        assert isinstance(key1, str)


# ---------------------------------------------------------------------------
# NEW: Fangraphs teams partial failure
# If one split (e.g. vs_lhp) returns a server error, the other splits must
# still be parsed and written. The scraper should not abort entirely.
# ---------------------------------------------------------------------------
from scrapers.mlb.fangraphs_teams import FangraphsTeamsScraper


class TestFangraphsTeamsPartialFailure:
    def _make_row(self, team: str) -> dict:
        return {
            "Team": team, "PA": "600", "AVG": ".260", "OBP": ".330",
            "SLG": ".430", "OPS": ".760", "ISO": ".170", "wOBA": ".330",
            "wRC+": "105", "K%": "22.0", "BB%": "8.5", "Barrel%": "8.0",
            "Hard%": "38.0", "GB%": "44.0", "FB%": "35.0", "SwStr%": "11.0",
        }

    def test_one_failed_split_does_not_crash_parse(self):
        """
        Simulate vs_lhp returning an empty list (as fetch() does after a caught error).
        parse() should log a warning and continue — overall and vs_rhp data survives.
        """
        scraper = FangraphsTeamsScraper.__new__(FangraphsTeamsScraper)
        scraper.config = {"season": 2026}

        raw = {
            "season": 2026,
            "splits": {
                "overall": [self._make_row("NYY"), self._make_row("BOS")],
                "vs_lhp": [],    # simulates a failed fetch
                "vs_rhp": [self._make_row("NYY"), self._make_row("BOS")],
            }
        }
        records = scraper.parse(raw)
        teams_in_records = {r["team"] for r in records}
        # overall + vs_rhp data must be present
        assert len(records) == 4
        assert "New York Yankees" in teams_in_records or "NYY" in teams_in_records

    def test_all_splits_empty_raises(self):
        """If all splits return empty, fetch() raises RuntimeError — nothing is written."""
        scraper = FangraphsTeamsScraper.__new__(FangraphsTeamsScraper)
        scraper.config = {"season": 2026}

        # Simulate fetch() having raised before all splits emptied —
        # test the guard in fetch() by calling it with a mock that always fails.
        import requests
        with patch.object(
            scraper.__class__,
            "_get_season",
            return_value=2026
        ):
            with patch("scrapers.mlb.fangraphs_teams._fetch_split", side_effect=RuntimeError("API down")):
                with pytest.raises(RuntimeError, match="all splits failed"):
                    scraper.fetch()


# ---------------------------------------------------------------------------
# NEW: Weather per-game failure isolation
# A forecast fetch failure for one stadium must not prevent the other games
# from having weather data written.
# ---------------------------------------------------------------------------
from scrapers.mlb.weather import WeatherScraper


class TestWeatherPartialFailure:
    def test_single_game_forecast_failure_doesnt_block_others(self):
        """
        Simulate a fetch() raw result where one game has weather_raw=None (fetch failed)
        and another has valid weather. parse() must return records for both.
        """
        scraper = WeatherScraper.__new__(WeatherScraper)
        scraper.config = {
            "season": 2026, "days_ahead": 1, "game_hour": 19,
            "bucket": "test-bucket", "gcs_object": "mlb/weather.json",
        }

        raw = {
            "games": [
                {
                    "game_id": "111",
                    "date": "2026-04-01",
                    "away_team": "New York Yankees",
                    "home_team": "Boston Red Sox",
                    "stadium": "Fenway Park",
                    "city": "Boston",
                    "state": "MA",
                    "is_dome": False,
                    "is_retractable": False,
                    "weather_raw": None,   # simulates failed fetch
                },
                {
                    "game_id": "222",
                    "date": "2026-04-01",
                    "away_team": "Los Angeles Dodgers",
                    "home_team": "San Francisco Giants",
                    "stadium": "Oracle Park",
                    "city": "San Francisco",
                    "state": "CA",
                    "is_dome": False,
                    "is_retractable": False,
                    "weather_raw": {
                        "temperature_f": 62.0,
                        "wind_mph": 14.0,
                        "wind_direction_deg": 270,
                        "precip_pct": 5.0,
                        "humidity_pct": 78.0,
                        "conditions": "Partly Cloudy",
                    },
                },
            ]
        }

        records = scraper.parse(raw)
        assert len(records) == 2, "Both games must produce a record"

        game_111 = next(r for r in records if r["game_id"] == "111")
        game_222 = next(r for r in records if r["game_id"] == "222")

        assert game_111["temperature_f"] is None
        assert game_111["wind_mph"] is None

        assert game_222["temperature_f"] == pytest.approx(62.0)
        assert game_222["wind_mph"] == pytest.approx(14.0)
        assert game_222["wind_direction"] == "W"

    def test_dome_game_always_null_weather(self):
        scraper = WeatherScraper.__new__(WeatherScraper)
        scraper.config = {
            "season": 2026, "days_ahead": 1, "game_hour": 19,
            "bucket": "test-bucket", "gcs_object": "mlb/weather.json",
        }
        raw = {
            "games": [{
                "game_id": "333",
                "date": "2026-04-01",
                "away_team": "New York Yankees",
                "home_team": "Tampa Bay Rays",
                "stadium": "Tropicana Field",
                "city": "St. Petersburg",
                "state": "FL",
                "is_dome": True,
                "is_retractable": False,
                "weather_raw": None,
            }]
        }
        records = scraper.parse(raw)
        assert len(records) == 1
        r = records[0]
        assert r["is_dome"] is True
        assert r["is_retractable"] is False
        assert r["temperature_f"] is None
        assert r["conditions"] == "Indoor"

    def test_retractable_roof_has_weather_data(self):
        """
        Retractable stadiums must NOT be treated as domes — weather is
        always fetched and passed through to GCS.
        """
        scraper = WeatherScraper.__new__(WeatherScraper)
        scraper.config = {
            "season": 2026, "days_ahead": 1, "game_hour": 19,
            "bucket": "test-bucket", "gcs_object": "mlb/weather.json",
        }
        raw = {
            "games": [{
                "game_id": "444",
                "date": "2026-04-01",
                "away_team": "Houston Astros",
                "home_team": "Seattle Mariners",
                "stadium": "T-Mobile Park",
                "city": "Seattle",
                "state": "WA",
                "is_dome": False,
                "is_retractable": True,
                "weather_raw": {
                    "temperature_f": 55.0,
                    "wind_mph": 8.0,
                    "wind_direction_deg": 0,
                    "precip_pct": 40.0,
                    "humidity_pct": 85.0,
                    "conditions": "Rain",
                },
            }]
        }
        records = scraper.parse(raw)
        assert len(records) == 1
        r = records[0]
        assert r["is_dome"] is False
        assert r["is_retractable"] is True
        assert r["temperature_f"] == pytest.approx(55.0)   # weather present, not null
        assert r["conditions"] != "Indoor"


# ---------------------------------------------------------------------------
# Failure on missing required columns
# ---------------------------------------------------------------------------
from scrapers.mlb.fangraphs_pitchers import FangraphsPitchersScraper
from scrapers.mlb.bullpen import BullpenScraper


class TestColumnValidation:
    def test_pitcher_parse_raises_on_missing_required_columns(self):
        scraper = FangraphsPitchersScraper.__new__(FangraphsPitchersScraper)
        scraper.config = {"season": 2026, "min_ip": 10}
        bad_raw = {"data": [{"PlayerName": "Test Pitcher", "Team": "NYY", "IP": "100.0"}]}
        with pytest.raises(RuntimeError, match="missing expected columns"):
            scraper.parse(bad_raw)

    def test_bullpen_parse_raises_on_missing_columns(self):
        scraper = BullpenScraper.__new__(BullpenScraper)
        scraper.config = {"season": 2026, "min_ip": 20}
        bad_raw = {"data": [{"Team": "NYY", "IP": "50.0"}]}
        with pytest.raises(RuntimeError, match="missing columns"):
            scraper.parse(bad_raw)
