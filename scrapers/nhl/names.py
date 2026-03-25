"""
scrapers/nhl/names.py
Single source of truth for NHL team name canonicalization.

All NHL scrapers must pass team names through to_canonical() before writing to GCS.
This ensures all NHL snapshots use identical team name strings so the Transform
layer can do direct dict lookups and join against the Action Network odds feed.

Review history:
  Round 1 (Gemini): initial implementation
  Round 2 (Claude): join key slug hardening with regex (Bug 7)
  Round 3 (both): confirmed correct — no further changes required

Usage:
    from scrapers.nhl.names import to_canonical, make_join_key, normalize_player_name

    to_canonical("TOR")              # → "Toronto Maple Leafs"
    to_canonical("STL Blues")        # → "St. Louis Blues"
    make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
    # → "2026-03-25_Boston_Bruins_Toronto_Maple_Leafs"
"""

from __future__ import annotations

import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical team names — 32 NHL franchises (2025-26 season)
# ---------------------------------------------------------------------------
CANONICAL_TEAMS: list[str] = [
    "Anaheim Ducks",
    "Boston Bruins",
    "Buffalo Sabres",
    "Calgary Flames",
    "Carolina Hurricanes",
    "Chicago Blackhawks",
    "Colorado Avalanche",
    "Columbus Blue Jackets",
    "Dallas Stars",
    "Detroit Red Wings",
    "Edmonton Oilers",
    "Florida Panthers",
    "Los Angeles Kings",
    "Minnesota Wild",
    "Montreal Canadiens",
    "Nashville Predators",
    "New Jersey Devils",
    "New York Islanders",
    "New York Rangers",
    "Ottawa Senators",
    "Philadelphia Flyers",
    "Pittsburgh Penguins",
    "San Jose Sharks",
    "Seattle Kraken",
    "St. Louis Blues",
    "Tampa Bay Lightning",
    "Toronto Maple Leafs",
    "Utah Hockey Club",
    "Vancouver Canucks",
    "Vegas Golden Knights",
    "Washington Capitals",
    "Winnipeg Jets",
]

# ---------------------------------------------------------------------------
# Alias map — covers abbreviations, partial names, alternate spellings,
# legacy names, and common source-specific variants.
# Keys are lowercased at lookup time.
# ---------------------------------------------------------------------------
_ALIAS_MAP: dict[str, str] = {
    # ── Anaheim Ducks ──────────────────────────────────────────────────────
    "anaheim ducks": "Anaheim Ducks", "anaheim": "Anaheim Ducks",
    "ducks": "Anaheim Ducks", "ana": "Anaheim Ducks",
    # ── Boston Bruins ──────────────────────────────────────────────────────
    "boston bruins": "Boston Bruins", "boston": "Boston Bruins",
    "bruins": "Boston Bruins", "bos": "Boston Bruins",
    # ── Buffalo Sabres ─────────────────────────────────────────────────────
    "buffalo sabres": "Buffalo Sabres", "buffalo": "Buffalo Sabres",
    "sabres": "Buffalo Sabres", "buf": "Buffalo Sabres",
    # ── Calgary Flames ─────────────────────────────────────────────────────
    "calgary flames": "Calgary Flames", "calgary": "Calgary Flames",
    "flames": "Calgary Flames", "cgy": "Calgary Flames", "cgi": "Calgary Flames",
    # ── Carolina Hurricanes ────────────────────────────────────────────────
    "carolina hurricanes": "Carolina Hurricanes", "carolina": "Carolina Hurricanes",
    "hurricanes": "Carolina Hurricanes", "canes": "Carolina Hurricanes", "car": "Carolina Hurricanes",
    # ── Chicago Blackhawks ─────────────────────────────────────────────────
    "chicago blackhawks": "Chicago Blackhawks", "chicago": "Chicago Blackhawks",
    "blackhawks": "Chicago Blackhawks", "hawks": "Chicago Blackhawks", "chi": "Chicago Blackhawks",
    # ── Colorado Avalanche ─────────────────────────────────────────────────
    "colorado avalanche": "Colorado Avalanche", "colorado": "Colorado Avalanche",
    "avalanche": "Colorado Avalanche", "avs": "Colorado Avalanche", "col": "Colorado Avalanche",
    # ── Columbus Blue Jackets ──────────────────────────────────────────────
    "columbus blue jackets": "Columbus Blue Jackets", "columbus": "Columbus Blue Jackets",
    "blue jackets": "Columbus Blue Jackets", "jackets": "Columbus Blue Jackets", "cbj": "Columbus Blue Jackets",
    # ── Dallas Stars ───────────────────────────────────────────────────────
    "dallas stars": "Dallas Stars", "dallas": "Dallas Stars",
    "stars": "Dallas Stars", "dal": "Dallas Stars",
    # ── Detroit Red Wings ──────────────────────────────────────────────────
    "detroit red wings": "Detroit Red Wings", "detroit": "Detroit Red Wings",
    "red wings": "Detroit Red Wings", "wings": "Detroit Red Wings", "det": "Detroit Red Wings",
    # ── Edmonton Oilers ────────────────────────────────────────────────────
    "edmonton oilers": "Edmonton Oilers", "edmonton": "Edmonton Oilers",
    "oilers": "Edmonton Oilers", "edm": "Edmonton Oilers",
    # ── Florida Panthers ───────────────────────────────────────────────────
    "florida panthers": "Florida Panthers", "florida": "Florida Panthers",
    "panthers": "Florida Panthers", "fla": "Florida Panthers",
    # ── Los Angeles Kings ──────────────────────────────────────────────────
    "los angeles kings": "Los Angeles Kings", "la kings": "Los Angeles Kings",
    "l.a. kings": "Los Angeles Kings", "kings": "Los Angeles Kings",
    "lak": "Los Angeles Kings", "laa": "Los Angeles Kings",
    # ── Minnesota Wild ─────────────────────────────────────────────────────
    "minnesota wild": "Minnesota Wild", "minnesota": "Minnesota Wild",
    "wild": "Minnesota Wild", "min": "Minnesota Wild",
    # ── Montreal Canadiens ─────────────────────────────────────────────────
    "montreal canadiens": "Montreal Canadiens", "montreal": "Montreal Canadiens",
    "canadiens": "Montreal Canadiens", "habs": "Montreal Canadiens", "mtl": "Montreal Canadiens",
    "montréal canadiens": "Montreal Canadiens", "montréal": "Montreal Canadiens",
    # ── Nashville Predators ────────────────────────────────────────────────
    "nashville predators": "Nashville Predators", "nashville": "Nashville Predators",
    "predators": "Nashville Predators", "preds": "Nashville Predators",
    "nsh": "Nashville Predators", "nas": "Nashville Predators",
    # ── New Jersey Devils ──────────────────────────────────────────────────
    "new jersey devils": "New Jersey Devils", "new jersey": "New Jersey Devils",
    "devils": "New Jersey Devils", "njd": "New Jersey Devils", "nj": "New Jersey Devils",
    # ── New York Islanders ─────────────────────────────────────────────────
    "new york islanders": "New York Islanders", "ny islanders": "New York Islanders",
    "islanders": "New York Islanders", "isles": "New York Islanders", "nyi": "New York Islanders",
    # ── New York Rangers ───────────────────────────────────────────────────
    "new york rangers": "New York Rangers", "ny rangers": "New York Rangers",
    "rangers": "New York Rangers", "nyr": "New York Rangers",
    # ── Ottawa Senators ────────────────────────────────────────────────────
    "ottawa senators": "Ottawa Senators", "ottawa": "Ottawa Senators",
    "senators": "Ottawa Senators", "sens": "Ottawa Senators", "ott": "Ottawa Senators",
    # ── Philadelphia Flyers ────────────────────────────────────────────────
    "philadelphia flyers": "Philadelphia Flyers", "philadelphia": "Philadelphia Flyers",
    "flyers": "Philadelphia Flyers", "phi": "Philadelphia Flyers",
    # ── Pittsburgh Penguins ────────────────────────────────────────────────
    "pittsburgh penguins": "Pittsburgh Penguins", "pittsburgh": "Pittsburgh Penguins",
    "penguins": "Pittsburgh Penguins", "pens": "Pittsburgh Penguins",
    "pit": "Pittsburgh Penguins", "pgh": "Pittsburgh Penguins",
    # ── San Jose Sharks ────────────────────────────────────────────────────
    "san jose sharks": "San Jose Sharks", "san jose": "San Jose Sharks",
    "sharks": "San Jose Sharks", "sjs": "San Jose Sharks", "sjo": "San Jose Sharks",
    # ── Seattle Kraken ─────────────────────────────────────────────────────
    "seattle kraken": "Seattle Kraken", "seattle": "Seattle Kraken",
    "kraken": "Seattle Kraken", "sea": "Seattle Kraken",
    # ── St. Louis Blues ────────────────────────────────────────────────────
    "st. louis blues": "St. Louis Blues", "st louis blues": "St. Louis Blues",
    "stl blues": "St. Louis Blues", "st. louis": "St. Louis Blues",
    "st louis": "St. Louis Blues", "blues": "St. Louis Blues", "stl": "St. Louis Blues",
    # ── Tampa Bay Lightning ────────────────────────────────────────────────
    "tampa bay lightning": "Tampa Bay Lightning", "tampa bay": "Tampa Bay Lightning",
    "tampa": "Tampa Bay Lightning", "lightning": "Tampa Bay Lightning",
    "bolts": "Tampa Bay Lightning", "tbl": "Tampa Bay Lightning", "tbay": "Tampa Bay Lightning",
    # ── Toronto Maple Leafs ────────────────────────────────────────────────
    "toronto maple leafs": "Toronto Maple Leafs", "toronto": "Toronto Maple Leafs",
    "maple leafs": "Toronto Maple Leafs", "leafs": "Toronto Maple Leafs", "tor": "Toronto Maple Leafs",
    # ── Utah Hockey Club ───────────────────────────────────────────────────
    "utah hockey club": "Utah Hockey Club", "utah": "Utah Hockey Club",
    "hockey club": "Utah Hockey Club", "uth": "Utah Hockey Club", "uta": "Utah Hockey Club",
    "utah mammoth": "Utah Hockey Club", "mammoth": "Utah Hockey Club",
    # formerly Arizona Coyotes — legacy aliases kept for historical data joins
    "arizona coyotes": "Utah Hockey Club", "arizona": "Utah Hockey Club",
    "coyotes": "Utah Hockey Club", "yotes": "Utah Hockey Club",
    "ari": "Utah Hockey Club", "phx": "Utah Hockey Club",
    # ── Vancouver Canucks ──────────────────────────────────────────────────
    "vancouver canucks": "Vancouver Canucks", "vancouver": "Vancouver Canucks",
    "canucks": "Vancouver Canucks", "van": "Vancouver Canucks",
    # ── Vegas Golden Knights ───────────────────────────────────────────────
    "vegas golden knights": "Vegas Golden Knights", "vegas": "Vegas Golden Knights",
    "golden knights": "Vegas Golden Knights", "knights": "Vegas Golden Knights",
    "vgk": "Vegas Golden Knights", "lvk": "Vegas Golden Knights",
    # ── Washington Capitals ────────────────────────────────────────────────
    "washington capitals": "Washington Capitals", "washington": "Washington Capitals",
    "capitals": "Washington Capitals", "caps": "Washington Capitals",
    "wsh": "Washington Capitals", "was": "Washington Capitals",
    # ── Winnipeg Jets ──────────────────────────────────────────────────────
    "winnipeg jets": "Winnipeg Jets", "winnipeg": "Winnipeg Jets",
    "jets": "Winnipeg Jets", "wpg": "Winnipeg Jets", "win": "Winnipeg Jets",
}

_CANONICAL_LOWER: set[str] = {t.lower() for t in CANONICAL_TEAMS}


def to_canonical(name: str, strict: bool = False) -> str:
    """Return the canonical NHL team name for *name*.

    Args:
        name:   Raw team name string from any source.
        strict: If True, raise ValueError on unknown input.
                If False (default), log WARNING and return original.

    Returns:
        Canonical team name string (e.g. "Toronto Maple Leafs").

    Raises:
        TypeError:  If name is not a str.
        ValueError: If strict=True and name cannot be resolved.
    """
    if not isinstance(name, str):
        raise TypeError(f"to_canonical expects str, got {type(name).__name__!r}")

    stripped = name.strip()
    lower = stripped.lower()

    # Fast path — already canonical
    if lower in _CANONICAL_LOWER:
        for canonical in CANONICAL_TEAMS:
            if canonical.lower() == lower:
                return canonical

    # Alias lookup
    canonical = _ALIAS_MAP.get(lower)
    if canonical:
        return canonical

    if strict:
        raise ValueError(
            f"NHL team name not recognized: {name!r}. "
            "Add alias to scrapers/nhl/names.py _ALIAS_MAP."
        )
    logger.warning("NHL team name not recognized (passing through): %r", name)
    return stripped


# ---------------------------------------------------------------------------
# Join key helper
# ---------------------------------------------------------------------------

def make_join_key(date: str, away_team: str, home_team: str) -> str:
    """Return a stable synthetic join key for games that lack an official game_id.

    Format: YYYY-MM-DD_<away_slug>_<home_slug>

    Slug rules:
      - All non-alphanumeric characters replaced with underscore
      - Consecutive underscores collapsed to one
      - Leading/trailing underscores stripped
      - The date string is NOT slugged — its hyphens are preserved

    Example:
      make_join_key("2026-03-25", "Boston Bruins", "Toronto Maple Leafs")
      → "2026-03-25_Boston_Bruins_Toronto_Maple_Leafs"

      make_join_key("2026-03-25", "St. Louis Blues", "Winnipeg Jets")
      → "2026-03-25_St_Louis_Blues_Winnipeg_Jets"

    The Transform layer uses this as a fallback join key when matching
    goalie/line data (which may not carry game_id) against the schedule.
    """
    def _slug(team: str) -> str:
        slug = re.sub(r'[^a-zA-Z0-9]', '_', team)
        slug = re.sub(r'_+', '_', slug)      # collapse consecutive underscores
        return slug.strip('_')

    return f"{date}_{_slug(away_team)}_{_slug(home_team)}"


# ---------------------------------------------------------------------------
# Player name helpers
# ---------------------------------------------------------------------------

def normalize_player_name(name: str) -> str:
    """Return a normalized comparison key for player name matching.

    Transformations applied:
      - Strips leading/trailing whitespace
      - Collapses internal whitespace runs to single space
      - Lowercases
      - Strips Unicode accents (NFD decompose → strip combining chars)
      - Hyphens preserved (not a Unicode combiner)

    The display name is preserved separately in each record. This key is
    suitable for matching across sources with differing accent handling
    (e.g. "Marc-André Fleury" → "marc-andre fleury").
    """
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", name)
    ascii_approx = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return " ".join(ascii_approx.lower().split())


def normalize_player_display(name: str) -> str:
    """Return a cleaned display name.

    Only whitespace normalization — no accent stripping, no case change.
    """
    if not name:
        return ""
    return " ".join(name.strip().split())
