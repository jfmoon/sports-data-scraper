class TennisResolver:
    def __init__(self):
        # Pre-seeded Top 20 WTA (Mandatory v4 Seeding)
        self.aliases = {
            "I. Swiatek": "Iga Swiatek",
            "A. Sabalenka": "Aryna Sabalenka",
            "C. Gauff": "Coco Gauff",
            "E. Rybakina": "Elena Rybakina",
            "J. Pegula": "Jessica Pegula",
            "O. Jabeur": "Ons Jabeur",
            "Q. Zheng": "Qinwen Zheng",
            "M. Sakkari": "Maria Sakkari",
            "J. Ostapenko": "Jelena Ostapenko",
            "D. Kasatkina": "Daria Kasatkina",
            "K. Muchova": "Karolina Muchova",
            "B. Krejcikova": "Barbora Krejcikova",
            "M. Keys": "Madison Keys",
            "L. Samsonova": "Ludmilla Samsonova",
            "E. Mertens": "Elise Mertens",
            "V. Azarenka": "Victoria Azarenka",
        }

    def resolve(self, raw_name: str) -> str:
        if not raw_name: return raw_name
        return self.aliases.get(raw_name, raw_name)