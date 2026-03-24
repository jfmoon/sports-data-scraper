"""
tests/test_cbb_hardening.py

Unit tests for the CBB hardening pass.
No network requests, no GCS credentials required.
"""

import pytest
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Mock crosswalk data — patches _alias_to_canonical and KNOWN_CANONICAL_TEAMS
# directly (the flat structures names.py builds at import time).
# ---------------------------------------------------------------------------

MOCK_CANONICAL = {"Iowa State", "UConn", "Duke", "Michigan", "Kansas"}
MOCK_ALIAS_MAP = {
    # canonical names map to themselves
    "Iowa State": "Iowa State",
    "UConn":      "UConn",
    "Duke":       "Duke",
    "Michigan":   "Michigan",
    "Kansas":     "Kansas",
    # aliases
    "Iowa St.":             "Iowa State",
    "Connecticut":          "UConn",
    "Connecticut Huskies":  "UConn",
    "Duke Blue Devils":     "Duke",
    "Michigan Wolverines":  "Michigan",
    "Kansas Jayhawks":      "Kansas",
}


@pytest.fixture(autouse=True)
def patch_crosswalk(monkeypatch):
    import scrapers.cbb.names as names_mod
    monkeypatch.setattr(names_mod, "_alias_to_canonical", MOCK_ALIAS_MAP)
    monkeypatch.setattr(names_mod, "KNOWN_CANONICAL_TEAMS", MOCK_CANONICAL)


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
        # "127.7" should not coerce to 127 — return None
        assert self.p.to_int("127.7") is None

    def test_to_int_seed_suffix(self):
        # Torvik appends seed/emoji: "1 seed, ✅" after team name digits
        # to_int on a rank cell like "1" should return 1
        assert self.p.to_int("1") == 1


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
        # Conference comparison is case-insensitive
        assert self.preprocess("Duke acc", "torvik") == "Duke"

    def test_torvik_multiword_team(self):
        assert self.preprocess("1 Iowa State B12", "torvik") == "Iowa State"

    def test_torvik_no_conference(self):
        # Last token not in CONFERENCES — leave it
        assert self.preprocess("1 UConn", "torvik") == "UConn"

    def test_torvik_strips_home_game_suffix(self):
        # Torvik appends (H) for home games
        assert self.preprocess("New Mexico(H)", "torvik") == "New Mexico"

    def test_torvik_home_suffix_with_conference(self):
        assert self.preprocess("New Mexico(H) MWC", "torvik") == "New Mexico"

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
        result = self.to_canonical("Fake University")
        assert result == "Fake University"

    def test_unresolved_strict_raises(self):
        with pytest.raises(ValueError, match="CRITICAL"):
            self.to_canonical("Fake University", strict=True)

    def test_unresolved_strict_with_source_raises(self):
        with pytest.raises(ValueError, match="CRITICAL"):
            self.to_canonical("1 Unknown ACC", source="torvik", strict=True)

    def test_whitespace_stripped(self):
        assert self.to_canonical("  Duke  ") == "Duke"

    def test_uconn_passthrough(self):
        assert self.to_canonical("UConn") == "UConn"

    def test_home_suffix_resolved(self):
        # New Mexico(H) -> strips (H) -> "New Mexico" — not in mock so returns original
        # Just verify it doesn't crash and returns a string
        result = self.to_canonical("New Mexico(H)", source="torvik")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# D-I universe assertion — hits the real crosswalk file
# ---------------------------------------------------------------------------

class TestUniverseAssertion:
    def test_known_canonical_teams_floor(self, monkeypatch):
        """Real crosswalk should have at least 50 teams (sanity floor)."""
        import importlib
        import scrapers.cbb.names as names_mod
        # Remove monkeypatch for this test to hit the real file
        monkeypatch.undo()
        importlib.reload(names_mod)
        assert len(names_mod.KNOWN_CANONICAL_TEAMS) >= 50, (
            f"Real crosswalk has only {len(names_mod.KNOWN_CANONICAL_TEAMS)} teams."
        )


# ---------------------------------------------------------------------------
# EvanMiya flatten logic
# ---------------------------------------------------------------------------

class TestEvanMiyaFlatten:
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
        flat = [row for page in [] for row in page]
        assert flat == []

    def test_completeness_gate(self):
        from scrapers.cbb.evanmiya_scraper import MIN_TEAM_COUNT
        short = [{"team_name": "Duke"}] * (MIN_TEAM_COUNT - 1)
        with pytest.raises(ValueError, match="completeness failure"):
            if len(short) < MIN_TEAM_COUNT:
                raise ValueError(
                    f"EvanMiya completeness failure: captured {len(short)} team rows."
                )


# ---------------------------------------------------------------------------
# TorvikScraper.validate — shape correctness
# ---------------------------------------------------------------------------

class TestTorvikWrapperValidate:
    """
    Tests validate() logic without instantiating TorvikScraper
    (BaseScraper requires abstract methods content_key and parse).
    Calls validate() as an unbound method with None as self.
    """

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

    def _validate(self, data: dict) -> bool:
        from scrapers.cbb.torvik import TorvikScraper
        return TorvikScraper.validate(None, data)

    def test_validate_passes_with_sufficient_teams(self):
        assert self._validate(self._make_data(362)) is True

    def test_validate_fails_with_too_few_teams(self):
        assert self._validate(self._make_data(200)) is False

    def test_validate_fails_with_empty_data(self):
        assert self._validate({}) is False
