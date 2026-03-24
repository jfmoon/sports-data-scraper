from scrapers.cbb.espn import ESPNScraper
from scrapers.cbb.action_network import ActionNetworkScraper
from scrapers.cbb.kenpom import KenPomScraper
from scrapers.cbb.evanmiya import EvanMiyaScraper
from scrapers.cbb.torvik import TorvikScraper
from scrapers.tennis.sofascore import SofaScoreScraper
from scrapers.tennis.the_odds_api import TheOddsApiScraper
from scrapers.tennis.tennisabstract import TennisAbstractScraper
from scrapers.sports.action_network import ActionNetworkOddsScraper
from scrapers.mlb.probables import MlbProbablesScraper
from scrapers.mlb.fangraphs_pitchers import FangraphsPitchersScraper
from scrapers.mlb.fangraphs_teams import FangraphsTeamsScraper
from scrapers.mlb.bullpen import BullpenScraper
from scrapers.mlb.statcast_pitchers import StatcastPitchersScraper
from scrapers.mlb.statcast_hitters import StatcastHittersScraper
from scrapers.mlb.weather import WeatherScraper
from scrapers.mlb.lineups import LineupsScraper

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
    # MLB
    "mlb_probables":         MlbProbablesScraper,
    "mlb_pitchers":          FangraphsPitchersScraper,
    "mlb_teams":             FangraphsTeamsScraper,
    "mlb_bullpen":           BullpenScraper,
    "mlb_statcast_pitchers": StatcastPitchersScraper,
    "mlb_statcast_hitters":  StatcastHittersScraper,
    "mlb_weather":           WeatherScraper,
    "mlb_lineups":           LineupsScraper,
}