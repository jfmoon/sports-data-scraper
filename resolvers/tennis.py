import json
import os


class TennisResolver:
    def __init__(self):
        crosswalk_path = os.path.join(
            os.path.dirname(__file__), "../data/crosswalks/tennis_players.json"
        )
        # Tennis crosswalk is optional — fall back to empty if not yet built
        self.NAME_MAP = {}
        if os.path.exists(crosswalk_path):
            with open(crosswalk_path) as f:
                data = json.load(f)
            for player in data.get("canonical", []):
                canonical = player["name"]
                for alias in player.get("aliases", []):
                    self.NAME_MAP[alias] = canonical
                self.NAME_MAP[canonical] = canonical
        else:
            # Pre-seeded top WTA aliases until crosswalk file is built
            self.NAME_MAP = {
                "I. Swiatek":    "Iga Swiatek",
                "A. Sabalenka":  "Aryna Sabalenka",
                "C. Gauff":      "Coco Gauff",
                "E. Rybakina":   "Elena Rybakina",
                "J. Pegula":     "Jessica Pegula",
                "O. Jabeur":     "Ons Jabeur",
                "Q. Zheng":      "Qinwen Zheng",
                "M. Sakkari":    "Maria Sakkari",
                "J. Ostapenko":  "Jelena Ostapenko",
                "D. Kasatkina":  "Daria Kasatkina",
                "K. Muchova":    "Karolina Muchova",
                "B. Krejcikova": "Barbora Krejcikova",
                "M. Keys":       "Madison Keys",
                "L. Samsonova":  "Ludmilla Samsonova",
                "E. Mertens":    "Elise Mertens",
                "V. Azarenka":   "Victoria Azarenka",
            }

    def resolve(self, raw_name: str) -> str:
        if not raw_name:
            return raw_name
        return self.NAME_MAP.get(raw_name, raw_name)
