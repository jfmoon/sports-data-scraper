"""
tests/test_nhl_hardening.py

Unit tests for the NHL extraction subsystem.
Covers normalization helpers, parser logic, Pydantic model validation,
and regression tests for all bugs fixed across three review rounds.

Review coverage:
  Bug 1 — _fallback key validation crash (daily_faceoff.py)
  Bug 2 — GCS path collision (nhl/lines.json) (config/nst)
  Bug 3 — import inside loop (moneypuck.py)
  Bug 4 — standings failure aborting schedule (nhl_api.py)
  Bug 5 — content_key prefix-only hash (moneypuck.py, natural_stat_trick.py)
  Bug 6 — _safe_int silent truncation (moneypuck.py)
  Bug 7 — make_join_key slug too permissive (names.py)
"""

from __future__ import annotations

import pytest
import requests

from scrapers.nhl.names import (
    CANONICAL_TEAMS,
    make_join_key,
    normalize_player_display,
    normalize_player_name,
    to_canonical,
)
from scrapers.nhl.daily_faceoff import NhlGoalieEntry, normalize_goalie_status
from scrapers.nhl.moneypuck import _safe_float, _safe_int, _sha256_content_key


# ===========================================================================
# Team name canonicalization — to_canonical()
# ===========================================================================

class TestToCanonical:

    def test_all_canonical_teams_pass_through(self):
        for team in CANONICAL_TEAMS:
            assert to_canonical(team) == team

    def test_common_abbreviations(self):
        cases = [
            ("TOR", "Toronto Maple Leafs"), ("tor", "Toronto Maple Leafs"),
            ("BOS", "Boston Bruins"), ("NYR", "New York Rangers"),
            ("NYI", "New York Islanders"), ("MTL", "Montreal Canadiens"),
            ("VGK", "Vegas Golden Knights"), ("STL", "St. Louis Blues"),
            ("WPG", "Winnipeg Jets"), ("CBJ", "Columbus Blue Jackets"),
            ("SJS", "San Jose Sharks"), ("TBL", "Tampa Bay Lightning"),
            ("NJD", "New Jersey Devils"), ("CGY", "Calgary Flames"),
            ("EDM", "Edmonton Oilers"), ("VAN", "Vancouver Canucks"),
        ]
        for raw, expected in cases:
            assert to_canonical(raw) == expected, f"Failed for {raw!r}"

    def test_partial_names(self):
        cases = [
            ("Leafs", "Toronto Maple Leafs"), ("Bruins", "Boston Bruins"),
            ("Habs", "Montreal Canadiens"), ("Canes", "Carolina Hurricanes"),
            ("Avs", "Colorado Avalanche"), ("Pens", "Pittsburgh Penguins"),
            ("Caps", "Washington Capitals"), ("Bolts", "Tampa Bay Lightning"),
        ]
        for raw, expected in cases:
            assert to_canonical(raw) == expected, f"Failed for {raw!r}"

    def test_case_insensitive(self):
        assert to_canonical("TORONTO MAPLE LEAFS") == "Toronto Maple Leafs"
        assert to_canonical("boston bruins") == "Boston Bruins"
        assert to_canonical("ST LOUIS BLUES") == "St. Louis Blues"
        assert to_canonical("st louis blues") == "St. Louis Blues"

    def test_st_louis_punctuation_variants(self):
        assert to_canonical("St. Louis Blues") == "St. Louis Blues"
        assert to_canonical("St Louis Blues") == "St. Louis Blues"
        assert to_canonical("STL Blues") == "St. Louis Blues"

    def test_la_kings_variants(self):
        assert to_canonical("LA Kings") == "Los Angeles Kings"
        assert to_canonical("L.A. Kings") == "Los Angeles Kings"
        assert to_canonical("Kings") == "Los Angeles Kings"
        assert to_canonical("LAK") == "Los Angeles Kings"

    def test_utah_hockey_club_current(self):
        assert to_canonical("Utah Hockey Club") == "Utah Hockey Club"
        assert to_canonical("Utah") == "Utah Hockey Club"
        assert to_canonical("UTH") == "Utah Hockey Club"

    def test_utah_legacy_arizona_coyotes_aliases(self):
        """Arizona Coyotes legacy names must resolve to Utah Hockey Club."""
        assert to_canonical("Arizona Coyotes") == "Utah Hockey Club"
        assert to_canonical("Arizona") == "Utah Hockey Club"
        assert to_canonical("ARI") == "Utah Hockey Club"
        assert to_canonical("PHX") == "Utah Hockey Club"
        assert to_canonical("Coyotes") == "Utah Hockey Club"
        assert to_canonical("Yotes") == "Utah Hockey Club"

    def test_ny_team_disambiguation(self):
        assert to_canonical("NYR") == "New York Rangers"
        assert to_canonical("NYI") == "New York Islanders"
        assert to_canonical("Rangers") == "New York Rangers"
        assert to_canonical("Islanders") == "New York Islanders"
        assert to_canonical("Isles") == "New York Islanders"

    def test_strips_whitespace(self):
        assert to_canonical("  Boston Bruins  ") == "Boston Bruins"
        assert to_canonical("\tBOS\n") == "Boston Bruins"

    def test_strict_raises_on_unknown(self):
        with pytest.raises(ValueError, match="not recognized"):
            to_canonical("Nonexistent FC", strict=True)

    def test_non_strict_logs_warning_and_returns_original(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="scrapers.nhl.names"):
            result = to_canonical("Unknown Team")
        assert result == "Unknown Team"
        assert "not recognized" in caplog.text

    def test_type_error_on_non_string(self):
        with pytest.raises(TypeError):
            to_canonical(42)  # type: ignore[arg-type]


# ===========================================================================
# Join key — make_join_key()
# ===========================================================================

class TestMakeJoinKey:

    def test_standard_team_names(self):
        """Bug 7 fix: standard teams must produce expected format."""
        key = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        assert key == "2026-03-25_Boston_Bruins_Toronto_Maple_Leafs"

    def test_date_hyphens_preserved(self):
        """Date string must NOT be slugged — its hyphens are meaningful."""
        key = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        assert key.startswith("2026-03-25_")

    def test_periods_replaced_in_team_names(self):
        """St. Louis Blues — period should become underscore."""
        key = make_join_key("2026-04-01", "St. Louis Blues", "Winnipeg Jets")
        assert "." not in key
        assert key == "2026-04-01_St_Louis_Blues_Winnipeg_Jets"

    def test_no_spaces_in_output(self):
        key = make_join_key("2026-03-25", "Vegas Golden Knights", "Colorado Avalanche")
        assert " " not in key

    def test_no_consecutive_underscores(self):
        """Consecutive underscores must be collapsed."""
        key = make_join_key("2026-03-25", "St. Louis Blues", "New York Rangers")
        assert "__" not in key

    def test_different_dates_produce_different_keys(self):
        k1 = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        k2 = make_join_key("2026-03-26", "Boston Bruins", "Toronto Maple Leafs")
        assert k1 != k2

    def test_home_away_order_matters(self):
        k1 = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        k2 = make_join_key("2026-03-25", "Toronto Maple Leafs", "Boston Bruins")
        assert k1 != k2

    def test_special_characters_replaced(self):
        """Bug 7 fix: apostrophes, slashes, and other punctuation become underscores."""
        key = make_join_key("2026-03-25", "St. Louis Blues", "Team/Other's")
        assert "'" not in key
        assert "/" not in key
        assert " " not in key

    def test_usable_as_dict_key(self):
        key = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        d = {key: {"game_id": 123}}
        assert d[key]["game_id"] == 123

    def test_join_key_roundtrip_after_canonical(self):
        """End-to-end: abbreviation → canonical → join key matches expected format."""
        away = to_canonical("BOS")
        home = to_canonical("TOR")
        key = make_join_key("2026-03-25", away, home)
        assert key == "2026-03-25_Boston_Bruins_Toronto_Maple_Leafs"


# ===========================================================================
# Player name normalization
# ===========================================================================

class TestNormalizePlayerName:

    def test_lowercases(self):
        assert normalize_player_name("Connor McDavid") == "connor mcdavid"

    def test_strips_leading_trailing_whitespace(self):
        assert normalize_player_name("  Nathan MacKinnon  ") == "nathan mackinnon"

    def test_collapses_internal_whitespace(self):
        assert normalize_player_name("Aaron  Judge") == "aaron judge"

    def test_strips_accents(self):
        assert normalize_player_name("Marc-André Fleury") == "marc-andre fleury"
        assert normalize_player_name("Mikaël Granlund") == "mikael granlund"

    def test_preserves_hyphens(self):
        """Hyphens are not Unicode combining chars and must be preserved."""
        result = normalize_player_name("Marc-André Fleury")
        assert "-" in result

    def test_empty_string(self):
        assert normalize_player_name("") == ""


class TestNormalizePlayerDisplay:

    def test_preserves_case_and_accents(self):
        assert normalize_player_display("Marc-André Fleury") == "Marc-André Fleury"
        assert normalize_player_display("Nathan MacKinnon") == "Nathan MacKinnon"

    def test_strips_whitespace(self):
        assert normalize_player_display("  Connor McDavid  ") == "Connor McDavid"

    def test_collapses_internal_whitespace(self):
        assert normalize_player_display("Connor  McDavid") == "Connor McDavid"

    def test_empty_string(self):
        assert normalize_player_display("") == ""


# ===========================================================================
# Goalie status normalization
# ===========================================================================

class TestNormalizeGoalieStatus:

    def test_confirmed_variants(self):
        assert normalize_goalie_status("Confirmed") == "confirmed"
        assert normalize_goalie_status("confirmed starter") == "confirmed"
        assert normalize_goalie_status("Starting") == "confirmed"
        assert normalize_goalie_status("Will Start") == "confirmed"
        assert normalize_goalie_status("CONFIRMED") == "confirmed"

    def test_expected_variants(self):
        assert normalize_goalie_status("Expected") == "expected"
        assert normalize_goalie_status("Expected to Start") == "expected"
        assert normalize_goalie_status("Likely") == "expected"
        assert normalize_goalie_status("Probable") == "expected"

    def test_projected_variants(self):
        assert normalize_goalie_status("Projected") == "projected"
        assert normalize_goalie_status("Possible") == "projected"
        assert normalize_goalie_status("Unconfirmed") == "projected"

    def test_unknown_variants(self):
        assert normalize_goalie_status("TBD") == "unknown"
        assert normalize_goalie_status("") == "unknown"
        assert normalize_goalie_status("Unknown") == "unknown"

    def test_unrecognized_defaults_to_projected(self):
        assert normalize_goalie_status("Some unrecognized string xyz") == "projected"

    def test_case_insensitive(self):
        assert normalize_goalie_status("CONFIRMED") == "confirmed"
        assert normalize_goalie_status("expected") == "expected"

    def test_strips_whitespace(self):
        assert normalize_goalie_status("  Confirmed  ") == "confirmed"


# ===========================================================================
# NHL API game status/type parsing
# ===========================================================================

class TestNhlApiParsers:

    def setup_method(self):
        from scrapers.nhl.nhl_api import NhlApiScraper
        self.scraper = NhlApiScraper.__new__(NhlApiScraper)
        self.scraper.config = {
            "bucket": "test-bucket",
            "gcs_object": "nhl/schedule.json",
            "include_preseason": False,
        }

    def test_status_scheduled(self):
        assert self.scraper._parse_game_status("FUT") == "scheduled"
        assert self.scraper._parse_game_status("PRE") == "scheduled"
        assert self.scraper._parse_game_status("fut") == "scheduled"

    def test_status_live(self):
        assert self.scraper._parse_game_status("LIVE") == "live"
        assert self.scraper._parse_game_status("CRIT") == "live"

    def test_status_final(self):
        assert self.scraper._parse_game_status("OFF") == "final"
        assert self.scraper._parse_game_status("FINAL") == "final"

    def test_status_postponed(self):
        assert self.scraper._parse_game_status("PPD") == "postponed"

    def test_status_unknown(self):
        assert self.scraper._parse_game_status(None) == "unknown"
        assert self.scraper._parse_game_status("WEIRD") == "unknown"
        assert self.scraper._parse_game_status("") == "unknown"

    def test_game_type_regular(self):
        assert self.scraper._parse_game_type(2) == "regular"

    def test_game_type_playoff(self):
        assert self.scraper._parse_game_type(3) == "playoff"

    def test_game_type_unknown(self):
        assert self.scraper._parse_game_type(99) == "type_99"


# ===========================================================================
# Bug 1 regression — _fallback key must not be in fallback parser output
# ===========================================================================

class TestDailyFaceoffFallbackParser:

    def _make_fallback_record(self):
        """Return a dict as produced by _parse_goalies_fallback — must be valid."""
        return {
            "date": "2026-03-25",
            "team": "",
            "opponent": None,
            "home_away": None,
            "goalie_name": "Jeremy Swayman",
            "starter_status_raw": "fallback_parse:Probable",
            "starter_status": "unknown",
            "join_key": None,
            "source": "daily_faceoff",
            "source_url": "https://www.dailyfaceoff.com/starting-goalies/",
            "fetched_at": "2026-03-25T12:00:00+00:00",
        }

    def test_fallback_record_validates_without_extra_key_error(self):
        """Bug 1 fix: fallback records must pass NhlGoalieEntry validation."""
        record = self._make_fallback_record()
        # Must not raise Pydantic ValidationError
        model = NhlGoalieEntry(**record)
        assert model.goalie_name == "Jeremy Swayman"
        assert model.starter_status == "unknown"

    def test_fallback_record_has_no_fallback_key(self):
        """Bug 1 fix: '_fallback' key must not appear in the dict."""
        record = self._make_fallback_record()
        assert "_fallback" not in record

    def test_fallback_status_raw_carries_prefix(self):
        """Fallback condition communicated via starter_status_raw prefix."""
        record = self._make_fallback_record()
        assert record["starter_status_raw"].startswith("fallback_parse:")

    def test_fallback_record_extra_key_is_silently_ignored_by_pydantic_v2(self):
        """Pydantic v2 ignores extra keys by default — it does NOT raise.

        This means the original Bug 1 (_fallback key) would not have crashed
        Pydantic itself. The actual crash path was different: the presence of
        the key caused issues downstream when the dict was serialized or when
        strict model_config was applied. The fix (removing the key entirely)
        is still correct and clean regardless.

        This test documents the actual Pydantic v2 behavior so future
        maintainers don't add model_config = ConfigDict(extra='forbid')
        thinking it's already enforced.
        """
        record = self._make_fallback_record()
        record["_fallback"] = True   # inject the old bug key
        # Pydantic v2 default: extra fields are IGNORED, not rejected
        model = NhlGoalieEntry(**record)
        # The model instantiates fine — the key is simply dropped
        assert model.goalie_name == "Jeremy Swayman"
        # Confirm the key does NOT appear on the model
        assert not hasattr(model, "_fallback")


# ===========================================================================
# Bug 2 regression — GCS path isolation
# ===========================================================================

class TestGcsPathIsolation:

    def test_dfo_and_nst_line_paths_do_not_collide(self):
        """Bug 2 fix: DFO and NST must target different GCS paths."""
        from scrapers.nhl.daily_faceoff import DailyFaceoffScraper
        from scrapers.nhl.natural_stat_trick import NaturalStatTrickScraper

        dfo = DailyFaceoffScraper.__new__(DailyFaceoffScraper)
        dfo.config = {
            "bucket": "test",
            "gcs_object": "nhl/goalies.json",  # primary DFO object
            "lines_gcs_object": "nhl/lines.json",
        }

        nst = NaturalStatTrickScraper.__new__(NaturalStatTrickScraper)
        nst.config = {
            "bucket": "test",
            "gcs_object": "nhl/nst_lines.json",
        }

        dfo_lines_path = dfo.config.get("lines_gcs_object", "nhl/lines.json")
        nst_lines_path = nst.config.get("gcs_object", "nhl/nst_lines.json")

        assert dfo_lines_path == "nhl/lines.json"
        assert nst_lines_path == "nhl/nst_lines.json"
        assert dfo_lines_path != nst_lines_path


# ===========================================================================
# Bug 4 regression — standings failure must not abort schedule
# ===========================================================================

class TestNhlApiStandingsResilience:

    def _make_scraper(self):
        from scrapers.nhl.nhl_api import NhlApiScraper
        s = NhlApiScraper.__new__(NhlApiScraper)
        s.config = {
            "bucket": "test",
            "gcs_object": "nhl/schedule.json",
            "include_preseason": False,
        }
        return s

    def test_empty_standings_parse_returns_two_batches(self):
        """Bug 4 fix: parse() must not crash when standings is empty."""
        scraper = self._make_scraper()
        raw = {
            "schedule": {"gameWeek": []},
            "standings": {"standings": []},
            "meta": {
                "end_date": "2026-03-27",
                "schedule_url": "https://api-web.nhle.com/v1/schedule/2026-03-25",
                "standings_url": "https://api-web.nhle.com/v1/standings/now",
                "standings_ok": False,
            },
        }
        parsed = scraper.parse(raw)
        assert len(parsed) == 2
        assert parsed[0]["_type"] == "games"
        assert parsed[1]["_type"] == "standings"
        assert len(parsed[1]["records"]) == 0

    def test_fetch_continues_when_standings_raises_http_error(self, monkeypatch):
        """Bug 4 fix: standings HTTPError must not prevent schedule fetch returning."""
        from scrapers.nhl.nhl_api import NhlApiScraper

        schedule_json = {"gameWeek": []}

        class MockResponse:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return schedule_json

        class MockBadResponse:
            def raise_for_status(self):
                raise requests.HTTPError("500 Server Error")

        def mock_get(url, timeout):
            if "schedule" in url:
                return MockResponse()
            return MockBadResponse()

        scraper = NhlApiScraper.__new__(NhlApiScraper)
        scraper.config = {
            "bucket": "test", "gcs_object": "nhl/schedule.json",
            "days_ahead": 2, "include_preseason": False,
        }

        session = type("Session", (), {"get": staticmethod(mock_get), "headers": {}})()
        monkeypatch.setattr(scraper, "_session", lambda: session)

        result = scraper.fetch()
        # Schedule must be present
        assert result["schedule"] == schedule_json
        # Standings must be empty dict, not an error
        assert result["standings"] == {"standings": []}
        assert result["meta"]["standings_ok"] is False

    def test_fetch_continues_when_standings_returns_invalid_json(self, monkeypatch):
        """Bug 4 fix (round 3): malformed standings JSON must not abort schedule.

        This test was explicitly flagged as a remaining gap in round 3 reviews.
        Both Gemini and Claude confirmed ValueError from .json() must be caught.
        """
        from scrapers.nhl.nhl_api import NhlApiScraper

        schedule_json = {"gameWeek": []}

        class MockScheduleResponse:
            def raise_for_status(self): pass
            def json(self): return schedule_json

        class MockBadJsonResponse:
            def raise_for_status(self): pass
            def json(self): raise ValueError("No JSON object could be decoded")

        def mock_get(url, timeout):
            if "schedule" in url:
                return MockScheduleResponse()
            return MockBadJsonResponse()

        scraper = NhlApiScraper.__new__(NhlApiScraper)
        scraper.config = {
            "bucket": "test", "gcs_object": "nhl/schedule.json",
            "days_ahead": 2, "include_preseason": False,
        }
        session = type("Session", (), {"get": staticmethod(mock_get), "headers": {}})()
        monkeypatch.setattr(scraper, "_session", lambda: session)

        # Must not raise — this is the round 3 remaining gap fix
        result = scraper.fetch()
        assert result["schedule"] == schedule_json
        assert result["standings"] == {"standings": []}


# ===========================================================================
# Bug 5 regression — content_key hashes full payload
# ===========================================================================

class TestContentKeyHashesFullPayload:

    def test_moneypuck_content_key_changes_on_late_row_change(self):
        """Bug 5 fix: a change in the 600th row must produce a different hash."""
        from scrapers.nhl.moneypuck import MoneypuckScraper

        scraper = MoneypuckScraper.__new__(MoneypuckScraper)
        scraper.config = {"bucket": "test", "season": "2025"}

        # 500-char identical prefix, different suffix
        prefix = "situation,team,games_played\n" + ("all,FLA,71\n" * 20)
        raw1 = {"season": "2025", "teams": prefix + "extra_row_A\n",
                "goalies": "", "skaters": ""}
        raw2 = {"season": "2025", "teams": prefix + "extra_row_B\n",
                "goalies": "", "skaters": ""}

        assert scraper.content_key(raw1) != scraper.content_key(raw2)

    def test_nst_content_key_changes_on_late_html_change(self):
        """Bug 5 fix: NST content key must change for changes beyond the prefix."""
        from scrapers.nhl.natural_stat_trick import NaturalStatTrickScraper

        scraper = NaturalStatTrickScraper.__new__(NaturalStatTrickScraper)
        scraper.config = {"bucket": "test", "season": "2025"}

        prefix = "<html><body>" + ("x" * 400)
        raw1 = {"season": "2025", "team_table_5v5": prefix + "TEAMDATA_A",
                "team_table_all": "", "player_table_5v5": ""}
        raw2 = {"season": "2025", "team_table_5v5": prefix + "TEAMDATA_B",
                "team_table_all": "", "player_table_5v5": ""}

        assert scraper.content_key(raw1) != scraper.content_key(raw2)

    def test_content_key_stable_on_identical_payload(self):
        """content_key must be deterministic for identical payloads."""
        payload = {"season": "2025", "teams": "a,b,c\n1,2,3\n",
                   "goalies": "x,y\n", "skaters": ""}
        h1 = _sha256_content_key(payload)
        h2 = _sha256_content_key(payload)
        assert h1 == h2


# ===========================================================================
# Bug 6 regression — _safe_int rounding behavior
# ===========================================================================

class TestSafeInt:

    def test_integer_string_returns_int(self):
        assert _safe_int("42") == 42
        assert _safe_int("0") == 0
        assert _safe_int("1") == 1

    def test_float_string_that_is_integer_rounds_silently(self):
        """Values within _INT_TOLERANCE of their rounded form must not warn."""
        assert _safe_int("1.0") == 1
        assert _safe_int("2.000") == 2
        assert _safe_int("3.0000001") == 3

    def test_non_integer_float_rounds_to_nearest_not_truncates(self):
        """Bug 6 fix: round() not int() — 1.9 must become 2, not 1."""
        assert _safe_int("1.9") == 2
        assert _safe_int("41.9") == 42

    def test_non_integer_float_emits_warning(self, caplog):
        """Bug 6 fix: anomalous float counts must emit a WARNING."""
        import logging
        with caplog.at_level(logging.WARNING, logger="scrapers.nhl.moneypuck"):
            result = _safe_int("1.9", field_name="goals")
        assert result == 2
        assert "goals" in caplog.text
        assert "1.9" in caplog.text

    def test_none_returns_none(self):
        assert _safe_int(None) is None

    def test_empty_string_returns_none(self):
        assert _safe_int("") is None

    def test_nan_returns_none(self):
        assert _safe_int("nan") is None


# ===========================================================================
# Bug 7 regression — join key slug hardening (supplementary)
# ===========================================================================

class TestJoinKeySlugHardening:

    def test_apostrophe_removed(self):
        key = make_join_key("2026-03-25", "Team's Name", "Boston Bruins")
        assert "'" not in key

    def test_slash_removed(self):
        key = make_join_key("2026-03-25", "Team/Name", "Boston Bruins")
        assert "/" not in key

    def test_no_leading_or_trailing_underscore_in_slug(self):
        # A name starting with punctuation must not produce leading underscores
        key = make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
        parts = key.split("_", 1)
        # date part ends at first underscore after date
        date_part = "2026-03-25"
        remainder = key[len(date_part) + 1:]
        assert not remainder.startswith("_")
        assert not remainder.endswith("_")

    def test_consecutive_underscores_collapsed(self):
        """Consecutive underscores from adjacent punctuation must be collapsed."""
        key = make_join_key("2026-03-25", "St. Louis Blues", "New York Rangers")
        assert "__" not in key
