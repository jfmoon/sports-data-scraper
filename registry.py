from scrapers.cbb.espn import ESPNScraper
from scrapers.cbb.action_network import ActionNetworkScraper
from scrapers.cbb.kenpom import KenPomScraper
from scrapers.cbb.evanmiya import EvanMiyaScraper
from scrapers.cbb.torvik import TorvikScraper
from scrapers.tennis.sofascore import SofaScoreScraper
from scrapers.tennis.the_odds_api import TheOddsApiScraper
from scrapers.tennis.tennisabstract import TennisAbstractScraper
from scrapers.sports.action_network import ActionNetworkOddsScraper

SCRAPER_REGISTRY = {
    # CBB
    "espn":             ESPNScraper,
    "action_network":   ActionNetworkScraper,
    "kenpom":           KenPomScraper,
    "evanmiya":         EvanMiyaScraper,
    "torvik":           TorvikScraper,
    # Tennis
    "sofascore":        SofaScoreScraper,
    "the_odds_api":     TheOddsApiScraper,
    "tennisabstract":   TennisAbstractScraper,
    # Sports odds
    "nba_odds":         ActionNetworkOddsScraper,
    "mlb_odds":         ActionNetworkOddsScraper,
    "nhl_odds":         ActionNetworkOddsScraper,
    "nfl_odds":         ActionNetworkOddsScraper,
}