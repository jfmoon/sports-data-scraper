class CBBResolver:
    def __init__(self):
        # Canonical names matching your App.jsx TEAM_DATA exactly
        self.CANONICAL = {
            "Akron", "Alabama", "Arizona", "Arkansas", "BYU", "Cal Baptist",
            "Clemson", "Duke", "Florida", "Furman", "Georgia", "Gonzaga",
            "Hawai'i", "High Point", "Hofstra", "Houston", "Howard", "Idaho",
            "Illinois", "Iowa", "Iowa State", "Kansas", "Kennesaw State",
            "Kentucky", "Lehigh", "Long Island University", "Louisville",
            "McNeese", "Miami (FL)", "Miami (Ohio)", "Michigan", "Michigan State",
            "Missouri", "NC State", "Nebraska", "North Carolina",
            "North Dakota State", "Northern Iowa", "Ohio State", "Penn",
            "Prairie View A&M", "Purdue", "Queens", "SMU", "Saint Louis",
            "Saint Mary's", "Santa Clara", "Siena", "South Florida", "St. John's",
            "TCU", "Tennessee", "Tennessee State", "Texas", "Texas A&M",
            "Texas Tech", "Troy", "UCF", "UCLA", "UConn", "UMBC", "Utah State",
            "VCU", "Vanderbilt", "Villanova", "Virginia", "Wisconsin", "Wright State",
        }

        # Raw Name -> Canonical Name map
        self.NAME_MAP = {
            "Howard Bison": "Howard",
            "NC State Wolfpack": "NC State",
            "UConn Huskies": "UConn",
            "Connecticut Huskies": "UConn",
            "Miami Hurricanes": "Miami (FL)",
            "Miami (FL) Hurricanes": "Miami (FL)",
            "Miami RedHawks": "Miami (Ohio)",
            "Miami (OH)": "Miami (Ohio)",
            "Saint Mary's Gaels": "Saint Mary's",
            "Saint Mary's (CA)": "Saint Mary's",
            # Add more aliases here iteratively
        }

    def resolve(self, raw_name: str) -> str:
        if not raw_name: return raw_name
        if raw_name in self.NAME_MAP: return self.NAME_MAP[raw_name]
        if raw_name in self.CANONICAL: return raw_name
        print(f"  [WARN] Unresolved CBB name: '{raw_name}'")
        return raw_name