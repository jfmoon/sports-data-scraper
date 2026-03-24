"""
tests/test_evanmiya.py

Unit tests for EvanMiya scraper helpers in evanmiya_scraper.py.

Covers:
  - header cleaning / tooltip stripping
  - header → field name mapping
  - float and int numeric parsing
  - table identification logic (header-based)
  - strict canonicalization failure behavior
  - content key stability and sensitivity
  - failure on missing required columns
  - failure on low row count
  - full normalize_row field coverage

All tests are pure unit tests. No Playwright / browser required.
"""

import hashlib
import json
import pytest

from scrapers.cbb.evanmiya_scraper import (
    _clean_header,
    _to_float,
    _to_int,
    _normalize_row,
    build_content_key,
    HEADER_MAP,
    REQUIRED_HEADERS,
    MIN_TEAM_COUNT,
    FLOAT_FIELDS,
    INT_FIELDS,
)


# ---------------------------------------------------------------------------
# Header cleaning
# ---------------------------------------------------------------------------

class TestCleanHeader:
    def test_plain_label(self):
        assert _clean_header("Team") == "Team"

    def test_strips_tooltip_json(self):
        raw = 'O-Rate{"x":{"opts":{"theme":"light","content":"Team Offensive Rating"}}}'
        assert _clean_header(raw) == "O-Rate"

    def test_strips_whitespace(self):
        assert _clean_header("  Net Rate  ") == "Net Rate"

    def test_net_rate_with_json(self):
        raw = 'Net Rate{"x":{"opts":{"content":"Team Net Relative Rating..."}}}'
        assert _clean_header(raw) == "Net Rate"

    def test_roster_rank_with_json(self):
        raw = 'Roster Rank{"x":{"opts":{"content":"Projected roster strength..."}}}'
        assert _clean_header(raw) == "Roster Rank"

    def test_empty_string(self):
        assert _clean_header("") == ""

    def test_only_json(self):
        # Degenerate case: no visible label before the brace
        assert _clean_header('{"x": {}}') == ""


# ---------------------------------------------------------------------------
# Header → field name mapping
# ---------------------------------------------------------------------------

class TestHeaderMap:
    def test_required_headers_all_mapped(self):
        for h in REQUIRED_HEADERS:
            assert h in HEADER_MAP, f"Required header '{h}' missing from HEADER_MAP"

    def test_rank_maps_to_rank(self):
        assert HEADER_MAP["Rank"] == "rank"

    def test_team_maps_to_name(self):
        assert HEADER_MAP["Team"] == "name"

    def test_o_rate_maps(self):
        assert HEADER_MAP["O-Rate"] == "o_rate"

    def test_d_rate_maps(self):
        assert HEADER_MAP["D-Rate"] == "d_rate"

    def test_net_rate_maps_to_relative_rating(self):
        assert HEADER_MAP["Net Rate"] == "relative_rating"

    def test_relative_rating_alias(self):
        assert HEADER_MAP["Relative Rating"] == "relative_rating"

    def test_opp_adjust_both_spellings(self):
        assert HEADER_MAP["Opp Adjust"] == "opp_adjust"
        assert HEADER_MAP["Opponent Adjust"] == "opp_adjust"

    def test_roster_rank_maps(self):
        assert HEADER_MAP["Roster Rank"] == "roster_rank"

    def test_optional_fields_present(self):
        for h in ("Pace Adjust", "Off Rank", "Def Rank", "True Tempo",
                  "Tempo Rank", "Injury Rank", "Home Rank"):
            assert h in HEADER_MAP, f"Optional header '{h}' missing from HEADER_MAP"

    def test_float_fields_are_float(self):
        for field in FLOAT_FIELDS:
            assert field not in INT_FIELDS, f"'{field}' is in both FLOAT_FIELDS and INT_FIELDS"

    def test_int_fields_are_int(self):
        for field in INT_FIELDS:
            assert field not in FLOAT_FIELDS, f"'{field}' is in both INT_FIELDS and FLOAT_FIELDS"


# ---------------------------------------------------------------------------
# Numeric parsing
# ---------------------------------------------------------------------------

class TestToFloat:
    def test_positive_float(self):
        assert _to_float("17.3") == pytest.approx(17.3)

    def test_negative_float(self):
        assert _to_float("-4.2") == pytest.approx(-4.2)

    def test_integer_string(self):
        assert _to_float("35") == pytest.approx(35.0)

    def test_with_comma(self):
        assert _to_float("1,234.5") == pytest.approx(1234.5)

    def test_empty_string(self):
        assert _to_float("") is None

    def test_non_numeric(self):
        assert _to_float("N/A") is None

    def test_none_input(self):
        assert _to_float(None) is None

    def test_dash(self):
        assert _to_float("-") is None


class TestToInt:
    def test_positive_int(self):
        assert _to_int("42") == 42

    def test_large_int(self):
        assert _to_int("365") == 365

    def test_float_string_returns_none(self):
        # We do NOT truncate floats silently
        assert _to_int("17.3") is None

    def test_with_comma(self):
        assert _to_int("1,000") == 1000

    def test_empty(self):
        assert _to_int("") is None

    def test_non_numeric(self):
        assert _to_int("N/A") is None

    def test_none_input(self):
        assert _to_int(None) is None


# ---------------------------------------------------------------------------
# Row normalization
# ---------------------------------------------------------------------------

class TestNormalizeRow:
    """Tests for _normalize_row using real canonical team names."""

    def test_basic_required_fields(self):
        raw = {
            "Rank": "1",
            "Team": "Duke",
            "O-Rate": "17.3",
            "D-Rate": "17.7",
            "Net Rate": "35.0",
        }
        record = _normalize_row(raw)
        assert record is not None
        assert record["name"] == "Duke"
        assert record["rank"] == 1
        assert record["o_rate"] == pytest.approx(17.3)
        assert record["d_rate"] == pytest.approx(17.7)
        assert record["relative_rating"] == pytest.approx(35.0)

    def test_optional_fields_captured(self):
        raw = {
            "Rank": "3",
            "Team": "Iowa State",
            "O-Rate": "14.1",
            "D-Rate": "12.5",
            "Net Rate": "26.6",
            "Opp Adjust": "1.2",
            "Roster Rank": "5",
            "Pace Adjust": "0.3",
            "Off Rank": "2",
            "Def Rank": "8",
        }
        record = _normalize_row(raw)
        assert record["opp_adjust"] == pytest.approx(1.2)
        assert record["roster_rank"] == 5
        assert record["pace_adjust"] == pytest.approx(0.3)
        assert record["off_rank"] == 2
        assert record["def_rank"] == 8

    def test_unmapped_columns_silently_skipped(self):
        raw = {
            "Team": "Michigan",
            "O-Rate": "10.0",
            "D-Rate": "9.0",
            "Net Rate": "19.0",
            "SomeUnknownColumn": "garbage",
        }
        record = _normalize_row(raw)
        assert record is not None
        assert "SomeUnknownColumn" not in record
        assert "someunknowncolumn" not in record

    def test_unresolved_team_name_raises(self):
        raw = {
            "Team": "ZZZZNOTATEAM",
            "O-Rate": "10.0",
            "D-Rate": "9.0",
            "Net Rate": "19.0",
        }
        with pytest.raises(ValueError):
            _normalize_row(raw)

    def test_no_team_column_returns_none(self):
        raw = {
            "O-Rate": "10.0",
            "D-Rate": "9.0",
        }
        result = _normalize_row(raw)
        assert result is None

    def test_evanmiya_source_preprocessing(self):
        # names.py strips parenthetical suffixes for source="evanmiya"
        # e.g. "Michigan (UM)" → "Michigan"
        # This test confirms the hook is being used correctly.
        raw = {
            "Team": "Michigan (UM)",
            "O-Rate": "12.0",
            "D-Rate": "10.0",
            "Net Rate": "22.0",
        }
        record = _normalize_row(raw)
        assert record is not None
        assert record["name"] == "Michigan"

    def test_relative_rating_alias(self):
        raw = {
            "Team": "Kansas",
            "O-Rate": "13.0",
            "D-Rate": "11.0",
            "Relative Rating": "24.0",
        }
        record = _normalize_row(raw)
        assert record["relative_rating"] == pytest.approx(24.0)

    def test_opponent_adjust_long_spelling(self):
        raw = {
            "Team": "Auburn",
            "O-Rate": "14.0",
            "D-Rate": "12.0",
            "Net Rate": "26.0",
            "Opponent Adjust": "-0.5",
        }
        record = _normalize_row(raw)
        assert record["opp_adjust"] == pytest.approx(-0.5)


# ---------------------------------------------------------------------------
# Content key
# ---------------------------------------------------------------------------

class TestBuildContentKey:
    def _make_records(self, n=5):
        teams = ["Auburn", "Duke", "Kansas", "Michigan", "UConn"][:n]
        return [
            {"name": t, "rank": i + 1, "o_rate": 10.0 + i, "d_rate": 9.0 + i,
             "relative_rating": 19.0 + i * 2}
            for i, t in enumerate(teams)
        ]

    def test_returns_64_char_hex(self):
        key = build_content_key(self._make_records())
        assert isinstance(key, str)
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_stable_across_calls(self):
        records = self._make_records()
        assert build_content_key(records) == build_content_key(records)

    def test_order_independent(self):
        records = self._make_records()
        shuffled = list(reversed(records))
        assert build_content_key(records) == build_content_key(shuffled)

    def test_sensitive_to_data_change(self):
        records = self._make_records()
        modified = [r.copy() for r in records]
        modified[0]["o_rate"] = 99.9
        assert build_content_key(records) != build_content_key(modified)

    def test_sensitive_to_team_name_change(self):
        records = self._make_records()
        modified = [r.copy() for r in records]
        modified[0]["name"] = "Gonzaga"
        assert build_content_key(records) != build_content_key(modified)

    def test_sensitive_to_roster_rank(self):
        records = self._make_records()
        with_rr = [dict(r, roster_rank=i + 1) for i, r in enumerate(records)]
        without_rr = records
        assert build_content_key(with_rr) != build_content_key(without_rr)

    def test_empty_list(self):
        # Should not crash; produces a valid hash of empty data
        key = build_content_key([])
        assert len(key) == 64


# ---------------------------------------------------------------------------
# Table identification logic (mocked Playwright element interface)
# ---------------------------------------------------------------------------

class MockEl:
    """Minimal mock for Playwright ElementHandle with inner_text() support."""
    def __init__(self, text):
        self._text = text

    def inner_text(self):
        return self._text


class MockTable:
    """Simulates a Reactable table with given header texts and row count."""
    def __init__(self, headers: list[str], row_count: int = 10):
        self._headers = headers
        self._row_count = row_count

    def query_selector_all(self, selector: str):
        if selector == ".rt-th":
            return [MockEl(h) for h in self._headers]
        if selector == ".rt-tr-group":
            return [object()] * self._row_count
        return []


def _simulate_find_table(tables):
    """
    Reimplementation of _find_team_ratings_table logic for pure unit testing
    without needing a real Playwright page object.
    """
    for i, table in enumerate(tables):
        header_els = table.query_selector_all(".rt-th")
        cleaned = {_clean_header(h.inner_text()) for h in header_els}
        has_team = "Team" in cleaned
        has_rate = bool(cleaned & {"O-Rate", "D-Rate"})
        if has_team and has_rate:
            return table
    return None


class TestFindTeamRatingsTable:
    def test_finds_correct_table(self):
        player_table = MockTable(["Rank", "Name", "Team", "OBPR", "DBPR", "BPR"])
        team_table   = MockTable(["Rank", "Team", "O-Rate", "D-Rate", "Net Rate"])
        resume_table = MockTable(["Team", "Resume Rank", "Win Quality Rank"])

        found = _simulate_find_table([player_table, team_table, resume_table])
        assert found is team_table

    def test_returns_none_when_not_found(self):
        player_table = MockTable(["Rank", "Name", "Team", "OBPR", "DBPR", "BPR"])
        resume_table = MockTable(["Team", "Resume Rank", "Win Quality Rank"])
        assert _simulate_find_table([player_table, resume_table]) is None

    def test_prefers_first_matching_table(self):
        table_a = MockTable(["Rank", "Team", "O-Rate", "D-Rate", "Net Rate"])
        table_b = MockTable(["Rank", "Team", "O-Rate", "D-Rate", "Roster Rank"])
        found = _simulate_find_table([table_a, table_b])
        assert found is table_a

    def test_accepts_d_rate_only(self):
        # A table with Team + D-Rate but not O-Rate should still match
        table = MockTable(["Team", "D-Rate", "Net Rate"])
        found = _simulate_find_table([table])
        assert found is table

    def test_accepts_full_authenticated_columns(self):
        full_headers = [
            "Rank", "Team", "O-Rate", "D-Rate", "Net Rate",
            "Opp Adjust", "Roster Rank", "Pace Adjust",
            "Off Rank", "Def Rank",
        ]
        table = MockTable(full_headers)
        assert _simulate_find_table([table]) is table

    def test_tooltip_json_in_headers_still_matches(self):
        # Headers as they appear in the real DOM with tooltip blobs appended
        table = MockTable([
            "Rank",
            "Team",
            'O-Rate{"x":{"opts":{"content":"Offensive Rate..."}}}',
            'D-Rate{"x":{"opts":{"content":"Defensive Rate..."}}}',
            'Net Rate{"x":{"opts":{"content":"Net Relative Rating..."}}}',
        ])
        assert _simulate_find_table([table]) is table


# ---------------------------------------------------------------------------
# Required column validation
# ---------------------------------------------------------------------------

class TestRequiredHeaders:
    def test_required_headers_set(self):
        assert REQUIRED_HEADERS == {"Team", "O-Rate", "D-Rate", "Relative Rating"}

    def test_min_team_count_is_substantial(self):
        assert MIN_TEAM_COUNT >= 300, (
            "MIN_TEAM_COUNT should be >= 300 to catch partial loads"
        )


# ---------------------------------------------------------------------------
# Additional tests for fixes applied after live inspection
# ---------------------------------------------------------------------------

class TestCleanCell:
    def test_plain_team_name(self):
        from scrapers.cbb.evanmiya_scraper import _clean_cell
        assert _clean_cell("Duke") == "Duke"

    def test_strips_emoji_and_json(self):
        from scrapers.cbb.evanmiya_scraper import _clean_cell
        raw = 'Duke \U0001f3c0{"x":{"opts":{"theme":"light","content":"Still alive"}}}'
        assert _clean_cell(raw) == "Duke"

    def test_strips_multiple_trailing_emoji(self):
        from scrapers.cbb.evanmiya_scraper import _clean_cell
        raw = 'Michigan \U0001f3c0\U0001f3c6{"x":{}}'
        assert _clean_cell(raw) == "Michigan"

    def test_no_emoji_passthrough(self):
        from scrapers.cbb.evanmiya_scraper import _clean_cell
        assert _clean_cell("Iowa State") == "Iowa State"

    def test_numeric_cell_unchanged(self):
        from scrapers.cbb.evanmiya_scraper import _clean_cell
        assert _clean_cell("17.3") == "17.3"


class TestRelativeRankingHeader:
    def test_relative_ranking_in_header_map(self):
        from scrapers.cbb.evanmiya_scraper import HEADER_MAP
        assert HEADER_MAP["Relative Ranking"] == "rank"

    def test_relative_rating_in_required_headers(self):
        from scrapers.cbb.evanmiya_scraper import REQUIRED_HEADERS
        assert "Relative Rating" in REQUIRED_HEADERS

    def test_full_table_headers_all_mapped(self):
        from scrapers.cbb.evanmiya_scraper import HEADER_MAP
        # All headers observed in the live full table
        live_headers = [
            "Relative Ranking", "Team", "O-Rate", "D-Rate",
            "Relative Rating", "Opponent Adjust", "Pace Adjust",
            "Off Rank", "Def Rank", "True Tempo", "Tempo Rank",
            "Injury Rank", "Home Rank", "Roster Rank",
        ]
        unmapped = [h for h in live_headers if h not in HEADER_MAP and h != "Team"]
        # Team maps to "name" via HEADER_MAP["Team"]
        assert "Team" in HEADER_MAP
        assert unmapped == [], f"Unmapped live headers: {unmapped}"

    def test_pane_selector_constant(self):
        # Ensures the scoping selector hasn't been removed
        import inspect
        import scrapers.cbb.evanmiya_scraper as m
        src = inspect.getsource(m._find_team_ratings_table)
        assert "shiny-tab-team_ratings" in src
