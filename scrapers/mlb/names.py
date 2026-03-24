"""
scrapers/mlb/names.py

Single source of truth for MLB team name canonicalization.
All MLB scrapers must call to_canonical() before writing team names to GCS.
Add new aliases here — nowhere else.
"""

# Canonical team names: full city+nickname, human-readable, stable.
CANONICAL_TEAMS = {
    "Arizona Diamondbacks",
    "Atlanta Braves",
    "Baltimore Orioles",
    "Boston Red Sox",
    "Chicago Cubs",
    "Chicago White Sox",
    "Cincinnati Reds",
    "Cleveland Guardians",
    "Colorado Rockies",
    "Detroit Tigers",
    "Houston Astros",
    "Kansas City Royals",
    "Los Angeles Angels",
    "Los Angeles Dodgers",
    "Miami Marlins",
    "Milwaukee Brewers",
    "Minnesota Twins",
    "New York Mets",
    "New York Yankees",
    "Oakland Athletics",
    "Philadelphia Phillies",
    "Pittsburgh Pirates",
    "San Diego Padres",
    "San Francisco Giants",
    "Seattle Mariners",
    "St. Louis Cardinals",
    "Tampa Bay Rays",
    "Texas Rangers",
    "Toronto Blue Jays",
    "Washington Nationals",
}

# All known non-canonical variants mapped to their canonical form.
# Keys: lowercase for case-insensitive lookup.
ALIAS_MAP: dict[str, str] = {
    # Abbreviations (MLB.com, Fangraphs, Baseball Savant, etc.)
    "ari": "Arizona Diamondbacks",
    "atl": "Atlanta Braves",
    "bal": "Baltimore Orioles",
    "bos": "Boston Red Sox",
    "chc": "Chicago Cubs",
    "cws": "Chicago White Sox",
    "cin": "Cincinnati Reds",
    "cle": "Cleveland Guardians",
    "col": "Colorado Rockies",
    "det": "Detroit Tigers",
    "hou": "Houston Astros",
    "kc":  "Kansas City Royals",
    "kcr": "Kansas City Royals",
    "laa": "Los Angeles Angels",
    "lad": "Los Angeles Dodgers",
    "mia": "Miami Marlins",
    "mil": "Milwaukee Brewers",
    "min": "Minnesota Twins",
    "nym": "New York Mets",
    "nyy": "New York Yankees",
    "oak": "Oakland Athletics",
    "phi": "Philadelphia Phillies",
    "pit": "Pittsburgh Pirates",
    "sd":  "San Diego Padres",
    "sdp": "San Diego Padres",
    "sf":  "San Francisco Giants",
    "sfg": "San Francisco Giants",
    "sea": "Seattle Mariners",
    "stl": "St. Louis Cardinals",
    "tb":  "Tampa Bay Rays",
    "tbr": "Tampa Bay Rays",
    "tex": "Texas Rangers",
    "tor": "Toronto Blue Jays",
    "wsh": "Washington Nationals",
    "was": "Washington Nationals",
    # Nickname-only variants
    "diamondbacks":      "Arizona Diamondbacks",
    "d-backs":           "Arizona Diamondbacks",
    "dbacks":            "Arizona Diamondbacks",
    "braves":            "Atlanta Braves",
    "orioles":           "Baltimore Orioles",
    "red sox":           "Boston Red Sox",
    "cubs":              "Chicago Cubs",
    "white sox":         "Chicago White Sox",
    "reds":              "Cincinnati Reds",
    "guardians":         "Cleveland Guardians",
    "indians":           "Cleveland Guardians",  # legacy
    "rockies":           "Colorado Rockies",
    "tigers":            "Detroit Tigers",
    "astros":            "Houston Astros",
    "royals":            "Kansas City Royals",
    "angels":            "Los Angeles Angels",
    "dodgers":           "Los Angeles Dodgers",
    "marlins":           "Miami Marlins",
    "brewers":           "Milwaukee Brewers",
    "twins":             "Minnesota Twins",
    "mets":              "New York Mets",
    "yankees":           "New York Yankees",
    "athletics":         "Oakland Athletics",
    "a's":               "Oakland Athletics",
    "phillies":          "Philadelphia Phillies",
    "pirates":           "Pittsburgh Pirates",
    "padres":            "San Diego Padres",
    "giants":            "San Francisco Giants",
    "mariners":          "Seattle Mariners",
    "cardinals":         "St. Louis Cardinals",
    "rays":              "Tampa Bay Rays",
    "rangers":           "Texas Rangers",
    "blue jays":         "Toronto Blue Jays",
    "nationals":         "Washington Nationals",
    # City-only variants
    "arizona":           "Arizona Diamondbacks",
    "atlanta":           "Atlanta Braves",
    "baltimore":         "Baltimore Orioles",
    "boston":            "Boston Red Sox",
    "colorado":          "Colorado Rockies",
    "detroit":           "Detroit Tigers",
    "houston":           "Houston Astros",
    "kansas city":       "Kansas City Royals",
    "miami":             "Miami Marlins",
    "milwaukee":         "Milwaukee Brewers",
    "minnesota":         "Minnesota Twins",
    "philadelphia":      "Philadelphia Phillies",
    "pittsburgh":        "Pittsburgh Pirates",
    "seattle":           "Seattle Mariners",
    "tampa bay":         "Tampa Bay Rays",
    "texas":             "Texas Rangers",
    "toronto":           "Toronto Blue Jays",
    "washington":        "Washington Nationals",
    # Fangraphs uses "LA Angels" / "LA Dodgers"
    "la angels":         "Los Angeles Angels",
    "la dodgers":        "Los Angeles Dodgers",
    "los angeles angels": "Los Angeles Angels",
    "los angeles dodgers": "Los Angeles Dodgers",
    # Baseball Savant sometimes uses full city
    "new york":          None,  # ambiguous — do not auto-resolve
    "chicago":           None,  # ambiguous — do not auto-resolve
    "st. louis":         "St. Louis Cardinals",
    "saint louis":       "St. Louis Cardinals",
    "san diego":         "San Diego Padres",
    "san francisco":     "San Francisco Giants",
    "oakland":           "Oakland Athletics",
    "cincinnati":        "Cincinnati Reds",
    "cleveland":         "Cleveland Guardians",
}


def to_canonical(name: str, strict: bool = False) -> str:
    """
    Normalize an MLB team name to its canonical form.

    - Returns canonical name if found.
    - Returns original (with warning) if not found and strict=False.
    - Raises ValueError if not found and strict=True.
    - Raises ValueError on ambiguous names (e.g. "New York", "Chicago") regardless of strict.
    """
    if not name:
        raise ValueError("to_canonical() received empty team name")

    # Already canonical — fast path.
    if name in CANONICAL_TEAMS:
        return name

    key = name.strip().lower()

    if key in ALIAS_MAP:
        resolved = ALIAS_MAP[key]
        if resolved is None:
            raise ValueError(
                f"MLB team name '{name}' is ambiguous and cannot be auto-resolved. "
                "Provide a more specific name."
            )
        return resolved

    if strict:
        raise ValueError(
            f"MLB team name '{name}' not found in canonical map. "
            "Add it to ALIAS_MAP in scrapers/mlb/names.py."
        )

    import logging
    logging.getLogger(__name__).warning(
        "MLB team name '%s' not in canonical map — using as-is. "
        "Add alias to scrapers/mlb/names.py.", name
    )
    return name
