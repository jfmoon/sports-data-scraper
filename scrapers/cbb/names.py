"""
scrapers/cbb/names.py

Centralized CBB team name canonicalization for the sports-data-scraper repo.

ALL CBB scrapers (kenpom.py, espn.py, action_network.py) must pass team names
through `to_canonical()` before writing to GCS. This ensures kenpom.json,
scores.json, and odds.json all use identical team name strings, eliminating
the need for fuzzy resolution in the analysis layer.

Canonical form: the name as it appears in ESPN and Action Network sources,
since those are less flexible than KenPom. KenPom abbreviations are mapped
to match the downstream sources.

To add a mapping: add an entry to ALIAS_MAP with the raw source name as key
and the canonical name as value. Do not maintain a separate crosswalk file —
this is the single source of truth for CBB team names.
"""

import logging

logger = logging.getLogger(__name__)

# ── Alias → Canonical map ─────────────────────────────────────────────────────
# Keys: raw names as they appear from any source (KenPom, ESPN, Action Network)
# Values: canonical name used in all GCS output
#
# Sourced from KenPom abbreviations vs ESPN/Action Network full names.
# Extend this list whenever a new mismatch is detected in pipeline logs.

ALIAS_MAP: dict[str, str] = {
    # KenPom abbreviations → canonical
    "Abilene Chr.":          "Abilene Christian",
    "Albany":                "Albany (NY)",
    "App State":             "Appalachian State",
    "Ark.-Pine Bluff":       "Arkansas-Pine Bluff",
    "Army West Point":       "Army",
    "Boise St.":             "Boise State",
    "Boston U.":             "Boston University",
    "Cal Baptist":           "California Baptist",
    "Cal Poly":              "Cal Poly",
    "Cal St. Bakersfield":   "Cal State Bakersfield",
    "Cal St. Fullerton":     "Cal State Fullerton",
    "Cal St. Northridge":    "Cal State Northridge",
    "Central Conn. St.":     "Central Connecticut",
    "Charleston So.":        "Charleston Southern",
    "Cleveland St.":         "Cleveland State",
    "Col. of Charleston":    "College of Charleston",
    "Colorado St.":          "Colorado State",
    "Connecticut":           "UConn",
    "Coppin St.":            "Coppin State",
    "Cornell":               "Cornell",
    "Eastern Ill.":          "Eastern Illinois",
    "Eastern Ky.":           "Eastern Kentucky",
    "Eastern Mich.":         "Eastern Michigan",
    "Eastern Wash.":         "Eastern Washington",
    "Fla. Atlantic":         "Florida Atlantic",
    "Fla. Gulf Coast":       "Florida Gulf Coast",
    "Florida St.":           "Florida State",
    "Fresno St.":            "Fresno State",
    "Ga. Southern":          "Georgia Southern",
    "Ga. Tech":              "Georgia Tech",
    "Gardner-Webb":          "Gardner-Webb",
    "Grambling":             "Grambling State",
    "Grand Canyon":          "Grand Canyon",
    "Idaho St.":             "Idaho State",
    "Illinois St.":          "Illinois State",
    "Indiana St.":           "Indiana State",
    "Iowa St.":              "Iowa State",
    "Jackson St.":           "Jackson State",
    "Jacksonville St.":      "Jacksonville State",
    "Kansas St.":            "Kansas State",
    "Kennesaw St.":          "Kennesaw State",
    "Kent St.":              "Kent State",
    "Long Island":           "LIU",
    "Loyola-Chicago":        "Loyola Chicago",
    "LSU":                   "LSU",
    "Md.-Eastern Shore":     "Maryland-Eastern Shore",
    "Miami (FL)":            "Miami",
    "Miami (OH)":            "Miami (OH)",
    "Michigan St.":          "Michigan State",
    "Middle Tenn.":          "Middle Tennessee",
    "Mississippi St.":       "Mississippi State",
    "Missouri St.":          "Missouri State",
    "Montana St.":           "Montana State",
    "Morehead St.":          "Morehead State",
    "Morgan St.":            "Morgan State",
    "Murray St.":            "Murray State",
    "N.C. A&T":              "North Carolina A&T",
    "N.C. Central":          "North Carolina Central",
    "N.C. State":            "NC State",
    "N.C.-Asheville":        "UNC Asheville",
    "N.C.-Greensboro":       "UNC Greensboro",
    "N.C.-Wilmington":       "UNC Wilmington",
    "N.J.I.T.":              "NJIT",
    "N.M. State":            "New Mexico State",
    "NC State":              "NC State",
    "New Mexico St.":        "New Mexico State",
    "Nicholls St.":          "Nicholls State",
    "Norfolk St.":           "Norfolk State",
    "North Dakota St.":      "North Dakota State",
    "Northern Ariz.":        "Northern Arizona",
    "Northern Colo.":        "Northern Colorado",
    "Northern Ill.":         "Northern Illinois",
    "Northern Iowa":         "Northern Iowa",
    "Northwestern St.":      "Northwestern State",
    "Ohio St.":              "Ohio State",
    "Oklahoma St.":          "Oklahoma State",
    "Oregon St.":            "Oregon State",
    "Penn St.":              "Penn State",
    "Portland St.":          "Portland State",
    "Prairie View":          "Prairie View A&M",
    "Sacramento St.":        "Sacramento State",
    "Saint Louis":           "Saint Louis",
    "Sam Houston":           "Sam Houston State",
    "Sam Houston St.":       "Sam Houston State",
    "San Diego St.":         "San Diego State",
    "San Jose St.":          "San Jose State",
    "Savannah St.":          "Savannah State",
    "SE Missouri St.":       "Southeast Missouri State",
    "Seattle U.":            "Seattle",
    "Seton Hall":            "Seton Hall",
    "SIU Edwardsville":      "SIU Edwardsville",
    "South Dakota St.":      "South Dakota State",
    "Southeast Mo. St.":     "Southeast Missouri State",
    "Southern Ill.":         "Southern Illinois",
    "Southern Miss.":        "Southern Miss",
    "Southern U.":           "Southern",
    "St. Bonaventure":       "St. Bonaventure",
    "St. John's":            "St. John's",
    "St. Peter's":           "Saint Peter's",
    "Stephen F. Austin":     "Stephen F. Austin",
    "Stetson":               "Stetson",
    "Stony Brook":           "Stony Brook",
    "TCU":                   "TCU",
    "Tennessee St.":         "Tennessee State",
    "Tennessee Tech":        "Tennessee Tech",
    "Texas A&M-CC":          "Texas A&M-Corpus Christi",
    "Texas A&M-Corpus Christi": "Texas A&M-Corpus Christi",
    "Texas Southern":        "Texas Southern",
    "Texas St.":             "Texas State",
    "Troy":                  "Troy",
    "UC Davis":              "UC Davis",
    "UC Irvine":             "UC Irvine",
    "UC Riverside":          "UC Riverside",
    "UC San Diego":          "UC San Diego",
    "UC Santa Barbara":      "UC Santa Barbara",
    "UCLA":                  "UCLA",
    "UIC":                   "Illinois-Chicago",
    "UL Monroe":             "Louisiana-Monroe",
    "UMass Lowell":          "UMass Lowell",
    "UNC":                   "North Carolina",
    "UNLV":                  "UNLV",
    "UT Arlington":          "UT Arlington",
    "UT Martin":             "UT Martin",
    "Utah St.":              "Utah State",
    "Utah Valley":           "Utah Valley",
    "UTEP":                  "UTEP",
    "UTSA":                  "UTSA",
    "VCU":                   "VCU",
    "VMI":                   "VMI",
    "W. Kentucky":           "Western Kentucky",
    "W. Michigan":           "Western Michigan",
    "Wichita St.":           "Wichita State",
    "Winston-Salem":         "Winston-Salem State",
    "Wis.-Green Bay":        "Green Bay",
    "Wis.-Milwaukee":        "Milwaukee",
    "Wright St.":            "Wright State",
    "Youngstown St.":        "Youngstown State",
}


def to_canonical(name: str) -> str:
    """
    Return the canonical team name for any input name.

    Performs an exact-match lookup in ALIAS_MAP first.
    If not found, returns the name unchanged (it is already canonical,
    or it is an unknown team that will surface as a join miss in logs).

    Args:
        name: Raw team name from any CBB scraper source.

    Returns:
        Canonical team name string.
    """
    if not name:
        return name
    canonical = ALIAS_MAP.get(name)
    if canonical is None:
        # Not in the alias map — name is either already canonical or unknown.
        # Unknown names will produce join misses in cbb_projector; add them here
        # when detected in pipeline logs.
        return name
    return canonical
