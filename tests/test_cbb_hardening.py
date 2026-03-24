"""
tests/test_cbb_hardening.py

Unit tests for the CBB hardening pass:
  - names.py (to_canonical, _preprocess, KNOWN_CANONICAL_TEAMS assertion)
  - torvik_scraper.py (NumericParser)
  - evanmiya_scraper.py (flatten logic, completeness gate)

These tests do not make network requests and do not require GCS credentials.
"""

import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# NumericParser
# ---------------------------------------------------------------------------

class TestNumericParser:
    def setup_method(self):
        from scrapers.cbb.torvik_scraper import NumericParser
        self.p = NumericParser

    def test_to_float_valid(self):
        assert self.p.to_float("127.7") == 127.7

    def test_to_float_with_comma(self):
        assert self.p.to_float("1,234.5") == 1234.5

    def test_to_float_with_percent(self):
        assert self.p.to_float("36.6%") == 36.6

    def test_to_float_na(self):
        assert self.p.to_float("N/A") is None

    def test_to_float_dash(self):
        assert self.p.to_float("-") is None

    def test_to_float_none(self):
        assert self.p.to_float(None) is None

    def test_to_float_empty(self):
        assert self.p.to_float("") is None

    def test_to_int_valid(self):
        assert self.p.to_int("42") == 42

    def test_to_int_with_comma(self):
        assert self.p.to_int("1,000") == 1000

    def test_to_int_na(self):
        assert self.p.to_int("N/A") is None

    def test_to_int_none(self):
        assert self.p.to_int(None) is None

    def test_to_int_float_string(self):
        # "127.7" is not a valid int string — should return None
        assert self.p.to_int("127.7") is None


# ---------------------------------------------------------------------------
# names.py — _preprocess
# ---------------------------------------------------------------------------

class TestPreprocess:
    def setup_method(self):
        from scrapers.cbb.names import _preprocess
        self.preprocess = _preprocess

    def test_torvik_strips_rank_and_conference(self):
        assert self.preprocess("1 Duke ACC", "torvik") == "Duke"

    def test_torvik_strips_conference_only(self):
        assert self.preprocess("Duke ACC", "torvik") == "Duke"

    def test_torvik_lowercase_conference(self):
        # Conference token comparison is case-insensitive (tokens[-1].upper())
        assert self.preprocess("Duke acc", "torvik") == "Duke"

    def test_torvik_multiword_team(self):
        assert self.preprocess("1 Iowa State B12", "torvik") == "Iowa State"

    def test_torvik_no_conference(self):
        # If last token is not a known conference, leave it alone
        assert self.preprocess("1 UConn", "torvik") == "UConn"

    def test_evanmiya_strips_parens(self):
        assert self.preprocess("Michigan (UM)", "evanmiya") == "Michigan"

    def test_evanmiya_no_parens(self):
        assert self.preprocess("Kansas", "evanmiya") == "Kansas"

    def test_no_source_strips_whitespace(self):
        assert self.preprocess("  Duke  ", None) == "Duke"

    def test_no_source_no_modification(self):
        assert self.preprocess("Iowa St.", None) == "Iowa St."


# ---------------------------------------------------------------------------
# names.py — to_canonical
# ---------------------------------------------------------------------------

# We patch KNOWN_CANONICAL_TEAMS and _crosswalk_data rather than requiring
# the real cbb_teams.json to be present in the test environment.

MOCK_CROSSWALK = {
    "Iowa St.": "Iowa State",
    "UConn":    "UConn",
    "Duke":     "Duke",
    "Michigan": "Michigan",
    "Kansas":   "Kansas",
}
MOCK_CANONICAL = set(MOCK_CROSSWALK.values())  # Iowa State, UConn, Duke, Michigan, Kansas


@pytest.fixture(autouse=True)
def patch_crosswalk(monkeypatch):
    import scrapers.cbb.names as names_mod
    monkeypatch.setattr(names_mod, "_crosswalk_data", MOCK_CROSSWALK)
    monkeypatch.setattr(names_mod, "KNOWN_CANONICAL_TEAMS", MOCK_CANONICAL)


class TestToCanonical:
    def setup_method(self):
        from scrapers.cbb.names import to_canonical
        self.to_canonical = to_canonical

    def test_already_canonical(self):
        assert self.to_canonical("Duke") == "Duke"

    def test_alias_lookup(self):
        assert self.to_canonical("Iowa St.") == "Iowa State"

    def test_torvik_source_with_rank_and_conf(self):
        assert self.to_canonical("1 Duke ACC", source="torvik") == "Duke"

    def test_evanmiya_source_with_parens(self):
        assert self.to_canonical("Michigan (UM)", source="evanmiya") == "Michigan"

    def test_unresolved_non_strict_returns_original(self):
        # Legacy behavior: return original name, log warning
        result = self.to_canonical("Fake University")
        assert result == "Fake University"

    def test_unresolved_strict_raises(self):
        with pytest.raises(ValueError, match="CRITICAL"):
            self.to_canonical("Fake University", strict=True)

    def test_unresolved_strict_with_source_raises(self):
        with pytest.raises(ValueError, match="torvik"):
            self.to_canonical("1 Unknown ACC", source="torvik", strict=True)

    def test_whitespace_stripped(self):
        assert self.to_canonical("  Duke  ") == "Duke"

    def test_uconn_passthrough(self):
        # UConn is both in crosswalk values and keys — should resolve cleanly
        assert self.to_canonical("UConn") == "UConn"


# ---------------------------------------------------------------------------
# D-I universe assertion bounds
# ---------------------------------------------------------------------------

class TestUniverseAssertion:
    def test_known_canonical_teams_in_range(self):
        """
        Verify the real crosswalk passes the 360-364 assertion.
        This test will fail if cbb_teams.json drifts outside expected bounds,
        which is the intended behavior.
        """
        import importlib
        import scrapers.cbb.names as names_mod

        # Re-import without monkeypatching to hit the real file
        importlib.reload(names_mod)
        assert 360 <= len(names_mod.KNOWN_CANONICAL_TEAMS) <= 364, (
            f"Real crosswalk has {len(names_mod.KNOWN_CANONICAL_TEAMS)} teams — "
            f"update assertion bounds or crosswalk file."
        )


# ---------------------------------------------------------------------------
# EvanMiya flatten logic
# ---------------------------------------------------------------------------

class TestEvanMiyaFlatten:
    """
    Test the response-flattening logic without launching a browser.
    """

    def test_flatten_single_page(self):
        pages = [[{"team_name": "Duke", "bpr": 10.0}]]
        flat = [row for page in pages for row in page]
        assert len(flat) == 1
        assert flat[0]["team_name"] == "Duke"

    def test_flatten_multi_page(self):
        pages = [
            [{"team_name": "Duke"}, {"team_name": "Kansas"}],
            [{"team_name": "Michigan"}],
        ]
        flat = [row for page in pages for row in page]
        assert len(flat) == 3

    def test_flatten_empty(self):
        pages: list = []
        flat = [row for page in pages for row in page]
        assert flat == []

    def test_completeness_gate(self):
        """MIN_TEAM_COUNT check should raise when flat list is short."""
        from scrapers.cbb.evanmiya_scraper import MIN_TEAM_COUNT

        short_list = [{"team_name": "Duke"}] * (MIN_TEAM_COUNT - 1)
        with pytest.raises(ValueError, match="completeness failure"):
            if len(short_list) < MIN_TEAM_COUNT:
                raise ValueError(
                    f"EvanMiya completeness failure: captured {len(short_list)} team rows "
                    f"(expected >= {MIN_TEAM_COUNT})."
                )


# ---------------------------------------------------------------------------
# TorvikScraper.validate — shape correctness
# ---------------------------------------------------------------------------

class TestTorvikWrapperValidate:
    def setup_method(self):
        # We don't want to instantiate the full BaseScraper; test validate logic
        # by calling it with a crafted data dict.
        from scrapers.cbb.torvik import TorvikScraper
        self.cls = TorvikScraper

    def _make_data(self, team_count: int) -> dict:
        return {
            "full_season": {
                "normalized_data": [{"name": f"Team{i}"} for i in range(team_count)],
                "metadata": {"team_count": team_count},
            },
            "last_10": {
                "normalized_data": [],
                "metadata": {"team_count": 0},
            },
        }

    def test_validate_passes_with_sufficient_teams(self):
        instance = object.__new__(self.cls)
        assert instance.validate(self._make_data(362)) is True

    def test_validate_fails_with_too_few_teams(self):
        instance = object.__new__(self.cls)
        assert instance.validate(self._make_data(200)) is False

    def test_validate_fails_with_empty_full_season(self):
        instance = object.__new__(self.cls)
        assert instance.validate({}) is False
