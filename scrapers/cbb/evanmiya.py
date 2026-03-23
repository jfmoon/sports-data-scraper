"""
EvanMiya scraper — PLACEHOLDER

EvanMiya (evanmiya.com) requires login. Implementation pending.
Disable in config.yaml until built out:
  evanmiya:
    enabled: false

Planned data points:
  - Team Offensive / Defensive Rating
  - Adjusted Tempo
  - Bayesian Performance Rating (BPR)
  - Player-level BPR contributions
"""
from base.scraper import BaseScraper


class EvanMiyaScraper(BaseScraper):

    def fetch(self):
        raise NotImplementedError("EvanMiya scraper not yet implemented.")

    def content_key(self, raw):
        return raw

    def parse(self, raw):
        return []

    def validate(self, records):
        return []

    def upsert(self, records):
        pass
