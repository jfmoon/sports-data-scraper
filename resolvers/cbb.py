import json
import os
import re


class CBBResolver:
    def __init__(self):
        crosswalk_path = os.path.join(
            os.path.dirname(__file__), "../data/crosswalks/cbb_teams.json"
        )
        with open(crosswalk_path) as f:
            data = json.load(f)

        # Build flat alias → canonical map at load time
        self.NAME_MAP = {}
        for team in data["canonical"]:
            canonical = team["name"]
            for alias in team["aliases"]:
                self.NAME_MAP[alias] = canonical
            # Always map canonical to itself
            self.NAME_MAP[canonical] = canonical

    def resolve(self, raw_name: str) -> str:
        if not raw_name:
            return raw_name

        # Direct lookup
        if raw_name in self.NAME_MAP:
            return self.NAME_MAP[raw_name]

        # Strip KenPom rank suffix e.g. "Duke 1" → "Duke"
        stripped = re.sub(r'\s+\d+$', '', raw_name).strip()
        if stripped in self.NAME_MAP:
            return self.NAME_MAP[stripped]

        print(f"  [WARN] Unresolved CBB name: '{raw_name}'")
        return raw_name
