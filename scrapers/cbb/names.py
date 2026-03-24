import json
import os
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load crosswalk
# ---------------------------------------------------------------------------
# Schema: {"canonical": [{"name": "Iowa State", "aliases": [...], "sources": {...}}, ...]}
# We expand this into two flat lookup structures at import time:
#   KNOWN_CANONICAL_TEAMS — set of canonical name strings for O(1) membership checks
#   _alias_to_canonical   — dict mapping every alias (and canonical name itself) → canonical name

CROSSWALK_PATH = os.path.join(os.path.dirname(__file__), "../../data/crosswalks/cbb_teams.json")
with open(CROSSWALK_PATH, "r") as f:
    _raw = json.load(f)

_teams: list = _raw.get("canonical", [])

# Build flat alias → canonical lookup.
# Includes: canonical name itself, all aliases[], all sources{} values.
_alias_to_canonical: dict[str, str] = {}
KNOWN_CANONICAL_TEAMS: set[str] = set()

for entry in _teams:
    canonical = entry["name"]
    KNOWN_CANONICAL_TEAMS.add(canonical)

    # canonical name maps to itself
    _alias_to_canonical[canonical] = canonical

    # all aliases
    for alias in entry.get("aliases", []):
        _alias_to_canonical[alias] = canonical

    # all source-specific strings (espn, action_network, kenpom, etc.)
    for src_name in entry.get("sources", {}).values():
        _alias_to_canonical[src_name] = canonical

# Crosswalk sanity check — guards against a completely empty or corrupted file.
# This crosswalk is built incrementally from live scraper output, not seeded
# with the full D-I universe, so we only assert a minimum floor.
assert len(KNOWN_CANONICAL_TEAMS) >= 50, (
    f"Crosswalk integrity failure: only {len(KNOWN_CANONICAL_TEAMS)} canonical teams loaded. "
    f"The file may be empty or malformed: {CROSSWALK_PATH}"
)

# ---------------------------------------------------------------------------
# Conference tokens for Torvik pre-processing
# ---------------------------------------------------------------------------
# Torvik appends the conference abbreviation after the team name; we strip it.
CONFERENCES = {
    "ACC", "SEC", "B10", "B12", "BE", "MWC", "AAC", "A10", "WCC",
    "SBC", "MVC", "CUSA", "MAC", "BSKY", "BW", "ASUN", "SC", "CAA",
    "MAAC", "OVC", "SUM", "SL", "WAC", "NEC", "MEAC", "SWAC",
    "IVY", "HORIZON", "AE",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _preprocess(name: str, source: str | None) -> str:
    """Apply source-specific cleaning before alias lookup."""
    cleaned = name.strip()
    if source == "torvik":
        if cleaned.endswith("(H)"):
            cleaned = cleaned[:-3].strip()
        tokens = cleaned.split()
        if tokens and tokens[0].isdigit():
            tokens = tokens[1:]          # drop leading rank number
        if tokens and tokens[-1].upper() in CONFERENCES:
            tokens = tokens[:-1]         # drop trailing conference token
        cleaned = " ".join(tokens)
    elif source == "evanmiya":
        cleaned = cleaned.split("(")[0].strip()
    return cleaned


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def to_canonical(name: str, source: str = None, strict: bool = False) -> str:
    """
    Resolve a raw team name to its canonical form.

    The crosswalk covers canonical names, all known aliases, and every
    source-specific string (ESPN, Action Network, KenPom, etc.), so a single
    lookup handles all cases.

    Legacy callers (ESPN, KenPom, Action Network):
        to_canonical(name)
        Returns the canonical string if found; otherwise logs a warning and
        returns the original name unchanged — preserves existing behavior.

    New strict callers (Torvik, EvanMiya):
        to_canonical(name, source="torvik", strict=True)
        Raises ValueError immediately on any unresolved name, halting the
        scrape before any GCS write occurs.

    Args:
        name:   Raw team name string from the source.
        source: Scraper identifier for source-specific pre-processing.
                Supported values: "torvik", "evanmiya". None = no pre-processing.
        strict: If True, raise ValueError on unresolved names instead of
                logging a warning and returning the original input.

    Returns:
        Canonical team name string.

    Raises:
        ValueError: Only when strict=True and the name cannot be resolved.
    """
    cleaned = _preprocess(name, source)

    # Single lookup covers canonical names, aliases, and source-specific strings.
    if cleaned in _alias_to_canonical:
        return _alias_to_canonical[cleaned]

    # Unresolved — strict scrapers halt; legacy scrapers log and continue.
    msg = (
        f"Unresolved CBB team name from source='{source or 'unknown'}': "
        f"'{name}' (cleaned: '{cleaned}'). "
        f"Add to the aliases[] or sources{{}} for the correct team in "
        f"data/crosswalks/cbb_teams.json."
    )
    if strict:
        raise ValueError(f"CRITICAL: {msg}")

    logger.warning(msg)
    return name  # return original input, not cleaned, to preserve legacy behavior
