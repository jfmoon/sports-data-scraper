from scrapers.cbb.espn import ESPNScraper
from scrapers.cbb.action_network import ActionNetworkScraper
from scrapers.cbb.kenpom import KenPomScraper
from scrapers.tennis.sofascore import SofaScoreScraper
from scrapers.tennis.the_odds_api import TheOddsApiScraper

SCRAPER_REGISTRY = {
    "espn": ESPNScraper,
    "action_network": ActionNetworkScraper,
    "kenpom": KenPomScraper,
    "sofascore": SofaScoreScraper,
    "the_odds_api": TheOddsApiScraper
}