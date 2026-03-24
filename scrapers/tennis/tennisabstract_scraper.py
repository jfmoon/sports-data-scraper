"""
WTA Tennis Abstract Scraper
Scrapes player stats from tennisabstract.com and emits raw float stats for downstream analysis.

Fetch method: Playwright + playwright-stealth (required — Cloudflare blocks headless Playwright
and curl_cffi without stealth patches).

Data sources (7 HTML tables, identified by column headers):
- Tour-Level Seasons  → Hld%, Brk%, A%, DF%, 1stIn, 1st%, 2nd%, RPW
- Winners & Errors    → Wnr/Pt, UFE/Pt, FH Wnr/Pt, BH Wnr/Pt, vs UFE/Pt
- Key Points          → BP Saved%, GP Conv%
- Charting: Serve     → Unret%, <=3 W%, RiP W%
- Charting: Return    → RiP%, RiP W%, RetWnr%, Slice%, FH/BH ratio
- Charting: Rally     → RallyLen, 1-3 W%, 10+ W%, BH Slice%, FHP/100, BHP/100
- Charting: Tactics   → SnV Freq, Net Freq, Net W%, Drop Freq, RallyAgg, ReturnAgg
- Recent Results      → Last 5 matches (date, tournament, opponent, score, W/L)

Output: raw_stats dict with 35 float fields. Rating computation is handled by
jfmoon/sports-analysis lib/logic/wta_mapper.py (Transform layer).
"""

try:
    from bs4 import BeautifulSoup
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth
except ImportError:
    print("❌ Missing dependencies. Install with: pip install playwright playwright-stealth beautifulsoup4")
    print("Then run: playwright install chromium")
    import sys; sys.exit(1)

import json
import time
import random
import re
import sys
import os
from datetime import datetime, timezone
from typing import Optional

# ── Top 100 WTA players (from tennisabstract.com/reports/wtaRankings.html, March 2026) ──
def fetch_rankings(top_n: int = 100) -> list:
    """Fetch live WTA rankings from Tennis Abstract. Falls back to FALLBACK_PLAYERS if unavailable."""
    url = "https://www.tennisabstract.com/reports/wtaRankings.html"
    print(f"\U0001f4cb Fetching live WTA rankings (top {top_n})...")
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector("table", timeout=30000)
            players = page.evaluate("""(topN) => {
                const tables = Array.from(document.querySelectorAll('table'));
                let rankTable = null;
                for (const t of tables) {
                    const rows = t.querySelectorAll('tr');
                    if (rows.length > 50) {
                        const firstCells = rows[1]?.querySelectorAll('td');
                        if (firstCells && firstCells[0]?.textContent.trim() === '1') {
                            rankTable = t; break;
                        }
                    }
                }
                if (!rankTable) return [];
                return Array.from(rankTable.querySelectorAll('tr')).slice(1, topN + 1).map(r => {
                    const cells = r.querySelectorAll('td');
                    const link = r.querySelector('a[href*="wplayer"]');
                    const match = link?.href.match(/p=([^&]+)/);
                    return {
                        rank: parseInt(cells[0]?.textContent.trim()),
                        name: link?.textContent.trim() || '',
                        slug: match ? match[1] : null,
                        country: cells[2]?.textContent.trim() || ''
                    };
                }).filter(p => p.rank && p.slug);
            }""", top_n)
            browser.close()
        print(f"\u2705 Fetched {len(players)} players from live rankings")
        return players
    except Exception as e:
        print(f"\u26a0\ufe0f  Could not fetch live rankings ({e}), using fallback list")
        return FALLBACK_PLAYERS[:top_n]


FALLBACK_PLAYERS = [
    {"rank": 1, "name": "Aryna Sabalenka", "slug": "ArynaSabalenka", "country": "BLR"},
    {"rank": 2, "name": "Iga Swiatek", "slug": "IgaSwiatek", "country": "POL"},
    {"rank": 3, "name": "Elena Rybakina", "slug": "ElenaRybakina", "country": "KAZ"},
    {"rank": 4, "name": "Coco Gauff", "slug": "CocoGauff", "country": "USA"},
    {"rank": 5, "name": "Jessica Pegula", "slug": "JessicaPegula", "country": "USA"},
    {"rank": 6, "name": "Amanda Anisimova", "slug": "AmandaAnisimova", "country": "USA"},
    {"rank": 7, "name": "Jasmine Paolini", "slug": "JasminePaolini", "country": "ITA"},
    {"rank": 8, "name": "Mirra Andreeva", "slug": "MirraAndreeva", "country": "RUS"},
    {"rank": 9, "name": "Elina Svitolina", "slug": "ElinaSvitolina", "country": "UKR"},
    {"rank": 10, "name": "Victoria Mboko", "slug": "VictoriaMboko", "country": "CAN"},
    {"rank": 11, "name": "Daria Kasatkina", "slug": "DariaKasatkina", "country": "RUS"},
    {"rank": 12, "name": "Ekaterina Alexandrova", "slug": "EkaterinaAlexandrova", "country": "RUS"},
    {"rank": 13, "name": "Karolina Muchova", "slug": "KarolinaMuchova", "country": "CZE"},
    {"rank": 14, "name": "Qinwen Zheng", "slug": "QinwenZheng", "country": "CHN"},
    {"rank": 15, "name": "Madison Keys", "slug": "MadisonKeys", "country": "USA"},
    {"rank": 16, "name": "Emma Navarro", "slug": "EmmaNavarro", "country": "USA"},
    {"rank": 17, "name": "Beatriz Haddad Maia", "slug": "BeatrizHaddadMaia", "country": "BRA"},
    {"rank": 18, "name": "Paula Badosa", "slug": "PaulaBadosa", "country": "ESP"},
    {"rank": 19, "name": "Anna Kalinskaya", "slug": "AnnaKalinskaya", "country": "RUS"},
    {"rank": 20, "name": "Barbora Krejcikova", "slug": "BarboraKrejcikova", "country": "CZE"},
    {"rank": 21, "name": "Jelena Ostapenko", "slug": "JelenaOstapenko", "country": "LAT"},
    {"rank": 22, "name": "Danielle Collins", "slug": "DanielleCollins", "country": "USA"},
    {"rank": 23, "name": "Donna Vekic", "slug": "DonnaVekic", "country": "CRO"},
    {"rank": 24, "name": "Liudmila Samsonova", "slug": "LiudmilaSamsonova", "country": "RUS"},
    {"rank": 25, "name": "Bernarda Pera", "slug": "BernardaPera", "country": "USA"},
    {"rank": 26, "name": "Eva Lys", "slug": "EvaLys", "country": "GER"},
    {"rank": 27, "name": "Peyton Stearns", "slug": "PeytonStearns", "country": "USA"},
    {"rank": 28, "name": "Ons Jabeur", "slug": "OnsJabeur", "country": "TUN"},
    {"rank": 29, "name": "Maria Sakkari", "slug": "MariaSakkari", "country": "GRE"},
    {"rank": 30, "name": "Karolina Pliskova", "slug": "KarolinaPliskova", "country": "CZE"},
    {"rank": 31, "name": "Clara Tauson", "slug": "ClaraTauson", "country": "DEN"},
    {"rank": 32, "name": "Veronika Kudermetova", "slug": "VeronikaKudermetova", "country": "RUS"},
    {"rank": 33, "name": "Anastasia Pavlyuchenkova", "slug": "AnastasiaPavlyuchenkova", "country": "RUS"},
    {"rank": 34, "name": "Anhelina Kalinina", "slug": "AnhelinaKalinina", "country": "UKR"},
    {"rank": 35, "name": "Marie Bouzkova", "slug": "MarieBouzkova", "country": "CZE"},
    {"rank": 36, "name": "Anna Blinkova", "slug": "AnnaBlinkova", "country": "RUS"},
    {"rank": 37, "name": "Elise Mertens", "slug": "EliseMertens", "country": "BEL"},
    {"rank": 38, "name": "Sloane Stephens", "slug": "SloaneStephens", "country": "USA"},
    {"rank": 39, "name": "Lesia Tsurenko", "slug": "LesiaTsurenko", "country": "UKR"},
    {"rank": 40, "name": "Diana Shnaider", "slug": "DianaShnaider", "country": "RUS"},
    {"rank": 41, "name": "Yulia Putintseva", "slug": "YuliaPutintseva", "country": "KAZ"},
    {"rank": 42, "name": "Xinyu Wang", "slug": "XinyuWang", "country": "CHN"},
    {"rank": 43, "name": "Magdalena Frech", "slug": "MagdalenaFrech", "country": "POL"},
    {"rank": 44, "name": "Leylah Fernandez", "slug": "LeylahFernandez", "country": "CAN"},
    {"rank": 45, "name": "Caroline Wozniacki", "slug": "CarolineWozniacki", "country": "DEN"},
    {"rank": 46, "name": "Tamara Korpatsch", "slug": "TamaraKorpatsch", "country": "GER"},
    {"rank": 47, "name": "Viktoriya Tomova", "slug": "ViktoriyaTomova", "country": "BUL"},
    {"rank": 48, "name": "Lauren Davis", "slug": "LaurenDavis", "country": "USA"},
    {"rank": 49, "name": "Bianca Andreescu", "slug": "BiancaAndreescu", "country": "CAN"},
    {"rank": 50, "name": "Yue Yuan", "slug": "YueYuan", "country": "CHN"},
    {"rank": 51, "name": "Linda Noskova", "slug": "LindaNoskova", "country": "CZE"},
    {"rank": 52, "name": "Alycia Parks", "slug": "AlyciaPark", "country": "USA"},
    {"rank": 53, "name": "Naomi Osaka", "slug": "NaomiOsaka", "country": "JPN"},
    {"rank": 54, "name": "Caroline Garcia", "slug": "CarolineGarcia", "country": "FRA"},
    {"rank": 55, "name": "Lucia Bronzetti", "slug": "LuciaBronzetti", "country": "ITA"},
    {"rank": 56, "name": "Sofia Kenin", "slug": "SofiaKenin", "country": "USA"},
    {"rank": 57, "name": "Camila Osorio", "slug": "CamilaOsorio", "country": "COL"},
    {"rank": 58, "name": "Anastasia Potapova", "slug": "AnastasiaPopova", "country": "RUS"},
    {"rank": 59, "name": "Nadia Podoroska", "slug": "NadiaPodoroska", "country": "ARG"},
    {"rank": 60, "name": "Rebeka Masarova", "slug": "RebekaMasarova", "country": "ESP"},
    {"rank": 61, "name": "Sara Sorribes Tormo", "slug": "SaraSorribesTormo", "country": "ESP"},
    {"rank": 62, "name": "Sorana Cirstea", "slug": "SoranaCirstea", "country": "ROU"},
    {"rank": 63, "name": "Irina-Camelia Begu", "slug": "IrinaCameliaBegu", "country": "ROU"},
    {"rank": 64, "name": "Katerina Siniakova", "slug": "KaterinaSiniakova", "country": "CZE"},
    {"rank": 65, "name": "Anastasia Zakharova", "slug": "AnastasiaZakharova", "country": "RUS"},
    {"rank": 66, "name": "Magda Linette", "slug": "MagdaLinette", "country": "POL"},
    {"rank": 67, "name": "Clara Burel", "slug": "ClaraBurel", "country": "FRA"},
    {"rank": 68, "name": "Oceane Dodin", "slug": "OceaneDodin", "country": "FRA"},
    {"rank": 69, "name": "Katerina Baindl", "slug": "KaterinaBindl", "country": "UKR"},
    {"rank": 70, "name": "Marketa Vondrousova", "slug": "MarketaVondrousova", "country": "CZE"},
    {"rank": 71, "name": "Mayar Sherif", "slug": "MayarSherif", "country": "EGY"},
    {"rank": 72, "name": "Harriet Dart", "slug": "HarrietDart", "country": "GBR"},
    {"rank": 73, "name": "Wang Xinyu", "slug": "WangXinyu", "country": "CHN"},
    {"rank": 74, "name": "Tatjana Maria", "slug": "TatjanaMaria", "country": "GER"},
    {"rank": 75, "name": "Ashlyn Krueger", "slug": "AshlynKrueger", "country": "USA"},
    {"rank": 76, "name": "Camila Giorgi", "slug": "CamilaGiorgi", "country": "ITA"},
    {"rank": 77, "name": "Jaqueline Cristian", "slug": "JaquelineCristian", "country": "ROU"},
    {"rank": 78, "name": "Petra Martic", "slug": "PetraMartic", "country": "CRO"},
    {"rank": 79, "name": "Irina Bara", "slug": "IrinaBara", "country": "ROU"},
    {"rank": 80, "name": "Kayla Day", "slug": "KaylaDay", "country": "USA"},
    {"rank": 81, "name": "Brenda Fruhvirtova", "slug": "BrendaFruhvirtova", "country": "CZE"},
    {"rank": 82, "name": "Suzan Lamens", "slug": "SuzanLamens", "country": "NED"},
    {"rank": 83, "name": "Taylor Townsend", "slug": "TaylorTownsend", "country": "USA"},
    {"rank": 84, "name": "Maddison Inglis", "slug": "MaddisonInglis", "country": "AUS"},
    {"rank": 85, "name": "Dalma Galfi", "slug": "DalmaGalfi", "country": "HUN"},
    {"rank": 86, "name": "Elena-Gabriela Ruse", "slug": "ElenaGabrielaRuse", "country": "ROU"},
    {"rank": 87, "name": "Elina Avanesyan", "slug": "ElinaAvanesyan", "country": "ARM"},
    {"rank": 88, "name": "Viktorija Golubic", "slug": "ViktorijaGolubic", "country": "SUI"},
    {"rank": 89, "name": "Daria Saville", "slug": "DariaSaville", "country": "AUS"},
    {"rank": 90, "name": "Renata Zarazua", "slug": "RenataZarazua", "country": "MEX"},
    {"rank": 91, "name": "Lulu Sun", "slug": "LuluSun", "country": "NZL"},
    {"rank": 92, "name": "Rebecca Sramkova", "slug": "RebeccaSramkova", "country": "SVK"},
    {"rank": 93, "name": "Greet Minnen", "slug": "GreetMinnen", "country": "BEL"},
    {"rank": 94, "name": "Katarzyna Kawa", "slug": "KatarzynaKawa", "country": "POL"},
    {"rank": 95, "name": "Martina Trevisan", "slug": "MartinaTrevisan", "country": "ITA"},
    {"rank": 96, "name": "Christina McHale", "slug": "ChristinaMcHale", "country": "USA"},
    {"rank": 97, "name": "Belinda Bencic", "slug": "BelindaBencic", "country": "SUI"},
    {"rank": 98, "name": "Anastasia Gasanova", "slug": "AnastasiaGasanova", "country": "RUS"},
    {"rank": 99, "name": "Nao Hibino", "slug": "NaoHibino", "country": "JPN"},
    {"rank": 100, "name": "Aliaksandra Sasnovich", "slug": "AliaksandraSasnovich", "country": "BLR"},
    {"rank": 101, "name": "Nikola Bartunkova", "slug": "NikolaBartunkova", "country": "CZE"},
    {"rank": 102, "name": "Veronika Erjavec", "slug": "VeronikaErjavec", "country": "SLO"},
    {"rank": 103, "name": "Talia Gibson", "slug": "TaliaGibson", "country": "AUS"},
    {"rank": 104, "name": "Yuliia Starodubtseva", "slug": "YuliiaStarodubtseva", "country": "UKR"},
    {"rank": 105, "name": "Sinja Kraus", "slug": "SinjaKraus", "country": "AUT"},
    {"rank": 106, "name": "Emiliana Arango", "slug": "EmilianaArango", "country": "COL"},
    {"rank": 107, "name": "Nuria Parrizas Diaz", "slug": "NuriaParrizasDiaz", "country": "ESP"},
    {"rank": 108, "name": "Cristina Bucsa", "slug": "CristinaBucsa", "country": "ESP"},
    {"rank": 109, "name": "Diane Parry", "slug": "DianeParry", "country": "FRA"},
    {"rank": 110, "name": "Alexandra Eala", "slug": "AlexandraEala", "country": "PHI"},
    {"rank": 111, "name": "Olga Danilovic", "slug": "OlgaDanilovic", "country": "SRB"},
    {"rank": 112, "name": "Danka Kovinic", "slug": "DankaKovinic", "country": "MNE"},
    {"rank": 113, "name": "Zeynep Sonmez", "slug": "ZeynepSonmez", "country": "TUR"},
    {"rank": 114, "name": "Kimberly Birrell", "slug": "KimberlyBirrell", "country": "AUS"},
    {"rank": 115, "name": "Elsa Jacquemot", "slug": "ElsaJacquemot", "country": "FRA"},
    {"rank": 116, "name": "Marta Kostyuk", "slug": "MartaKostyuk", "country": "UKR"},
    {"rank": 117, "name": "Tereza Martincova", "slug": "TerezaMartincova", "country": "CZE"},
    {"rank": 118, "name": "Moyuka Uchijima", "slug": "MoyukaUchijima", "country": "JPN"},
    {"rank": 119, "name": "Zhu Lin", "slug": "ZhuLin", "country": "CHN"},
    {"rank": 120, "name": "Ana Bogdan", "slug": "AnaBogdan", "country": "ROU"},
    {"rank": 121, "name": "Varvara Gracheva", "slug": "VarvaraGracheva", "country": "FRA"},
    {"rank": 122, "name": "Arantxa Rus", "slug": "ArantxaRus", "country": "NED"},
    {"rank": 123, "name": "Yafan Wang", "slug": "YafanWang", "country": "CHN"},
    {"rank": 124, "name": "Fernanda Contreras Gomez", "slug": "FernandaContrerasGomez", "country": "MEX"},
    {"rank": 125, "name": "Polona Hercog", "slug": "PolonaHercog", "country": "SLO"},
    {"rank": 126, "name": "Ana Konjuh", "slug": "AnaKonjuh", "country": "CRO"},
    {"rank": 127, "name": "Panna Udvardy", "slug": "PannaUdvardy", "country": "HUN"},
    {"rank": 128, "name": "Claire Liu", "slug": "ClaireLiu", "country": "USA"},
    {"rank": 129, "name": "Kristina Mladenovic", "slug": "KristinaMladenovic", "country": "FRA"},
    {"rank": 130, "name": "Erika Andreeva", "slug": "ErikaAndreeva", "country": "RUS"},
    {"rank": 131, "name": "Caty McNally", "slug": "CatyMcNally", "country": "USA"},
    {"rank": 132, "name": "Anna Siskova", "slug": "AnnaSiskova", "country": "CZE"},
    {"rank": 133, "name": "Jil Teichmann", "slug": "JilTeichmann", "country": "SUI"},
    {"rank": 134, "name": "Alize Cornet", "slug": "AlizeCornet", "country": "FRA"},
    {"rank": 135, "name": "Laura Pigossi", "slug": "LauraPigossi", "country": "BRA"},
    {"rank": 136, "name": "Priscilla Hon", "slug": "PriscillaHon", "country": "AUS"},
    {"rank": 137, "name": "Valentini Grammatikopoulou", "slug": "ValentiniGrammatikopoulou", "country": "GRE"},
    {"rank": 138, "name": "Yanina Wickmayer", "slug": "YaninaWickmayer", "country": "BEL"},
    {"rank": 139, "name": "Despina Papamichail", "slug": "DespinaPapamichail", "country": "GRE"},
    {"rank": 140, "name": "Anastasia Tikhonova", "slug": "AnastasiaTikhonova", "country": "RUS"},
    {"rank": 141, "name": "Anastasia Grymalska", "slug": "AnastasiaGrymalska", "country": "ITA"},
    {"rank": 142, "name": "Katherine Sebov", "slug": "KatherineSebov", "country": "CAN"},
    {"rank": 143, "name": "Anna Karolina Schmiedlova", "slug": "AnnaKarolinaSchmiedlova", "country": "SVK"},
    {"rank": 144, "name": "Victoria Azarenka", "slug": "VictoriaAzarenka", "country": "BLR"},
    {"rank": 145, "name": "Iryna Shymanovich", "slug": "IrynaShymanovich", "country": "BLR"},
    {"rank": 146, "name": "Chantal Skamlova", "slug": "ChantalSkamlova", "country": "SVK"},
    {"rank": 147, "name": "Linda Fruhvirtova", "slug": "LindaFruhvirtova", "country": "CZE"},
    {"rank": 148, "name": "Fiona Ferro", "slug": "FionaFerro", "country": "FRA"},
    {"rank": 149, "name": "Weronika Falkowska", "slug": "WeronikaFalkowska", "country": "POL"},
    {"rank": 150, "name": "Jessica Bouzas Maneiro", "slug": "JessicaBouzasManeiro", "country": "ESP"},
    {"rank": 151, "name": "Timea Babos", "slug": "TimeaBabos", "country": "HUN"},
    {"rank": 152, "name": "Destanee Aiava", "slug": "DestaneeAiava", "country": "AUS"},
    {"rank": 153, "name": "Ena Shibahara", "slug": "EnaShibahara", "country": "JPN"},
    {"rank": 154, "name": "Maria Timofeeva", "slug": "MariaTimofeeva", "country": "RUS"},
    {"rank": 155, "name": "Heather Watson", "slug": "HeatherWatson", "country": "GBR"},
    {"rank": 156, "name": "Storm Hunter", "slug": "StormHunter", "country": "AUS"},
    {"rank": 157, "name": "Anastasia Gasanova", "slug": "AnastasiaGasanova2", "country": "RUS"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def pct(s: str) -> Optional[float]:
    """Convert '75.2%' → 75.2, return None if not parseable."""
    if not s:
        return None
    s = s.strip().rstrip('%')
    try:
        return float(s)
    except ValueError:
        return None

def val(s: str) -> Optional[float]:
    """Convert a plain float/int string → float."""
    if not s:
        return None
    try:
        return float(s.strip())
    except ValueError:
        return None

def clamp(v: float, lo: float = 1.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, v))

def normalize(raw: Optional[float], lo: float, hi: float,
              invert: bool = False, scale: float = 10.0) -> Optional[float]:
    """Linear map raw ∈ [lo, hi] → [1, scale], optionally inverted."""
    if raw is None:
        return None
    frac = (raw - lo) / (hi - lo) if hi != lo else 0.5
    frac = max(0.0, min(1.0, frac))
    if invert:
        frac = 1.0 - frac
    return clamp(round(1 + frac * (scale - 1), 1))

def normalize_text(s: str) -> str:
    """Replace non-breaking spaces and other whitespace variants with regular spaces."""
    return s.replace('\xa0', ' ').replace('\u200b', '').strip()


def find_table_by_columns(tables: list, *required_columns: str) -> Optional[object]:
    """
    Find the first table whose header row contains ALL of the required column names.
    This is robust to page structure changes — never relies on table index position.
    """
    for t in tables:
        rows = t.find_all("tr")
        if not rows:
            continue
        header_text = normalize_text(rows[0].get_text(" "))
        if all(col in header_text for col in required_columns):
            return t
    return None


def get_table_career_row(tables: list, *required_columns: str) -> dict:
    """
    Find a table by its unique column headers, then extract the Career
    (or Last-52-Weeks) summary row as a {header: value} dict.
    """
    t = find_table_by_columns(tables, *required_columns)
    if t is None:
        return {}

    rows = t.find_all("tr")
    if not rows:
        return {}

    # Extract header row — normalize non-breaking spaces
    header_row = rows[0]
    headers = [normalize_text(th.get_text()) for th in header_row.find_all(["th", "td"])]

    # Find Career row (starts with "Career")
    data_row = None
    for row in rows:
        cells = row.find_all("td")
        if cells and normalize_text(cells[0].get_text()).startswith("Career"):
            data_row = cells
            break

    # Fallback: Last 52 Weeks
    if data_row is None:
        for row in rows:
            cells = row.find_all("td")
            if cells and "Last 52 Weeks" in normalize_text(cells[0].get_text()):
                data_row = cells
                break

    if data_row is None:
        return {}

    values = [normalize_text(c.get_text()) for c in data_row]
    return dict(zip(headers, values))


def fetch_player_page(slug: str, page=None) -> Optional[BeautifulSoup]:
    """
    Fetch a Tennis Abstract player page using Playwright + stealth.

    curl_cffi is no longer used — Cloudflare blocks it consistently.
    Stealth Playwright is the only reliable fetch method.

    Pass a persistent Playwright page object to reuse across multiple players
    (avoids launching a new browser per player). If None, creates a one-shot
    browser for single-player mode.
    """
    url = f"https://www.tennisabstract.com/cgi-bin/wplayer.cgi?p={slug}"

    def _fetch_with_page(pw_page) -> Optional[BeautifulSoup]:
        try:
            pw_page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                pw_page.wait_for_selector("table", timeout=10000)
            except Exception:
                pass
            html = pw_page.content()
            soup = BeautifulSoup(html, "html.parser")
            if len(soup.find_all("table")) >= 5:
                return soup
            print(f"  ⚠️  Playwright+stealth: insufficient table data for {slug}")
            return None
        except Exception as e:
            print(f"  ❌ Playwright fetch failed for {slug}: {e}", file=sys.stderr)
            return None

    if page is not None:
        return _fetch_with_page(page)

    # Single-player mode — create a one-shot browser
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            pw_page = context.new_page()
            Stealth().apply_stealth_sync(pw_page)
            result = _fetch_with_page(pw_page)
            browser.close()
        return result
    except Exception as e:
        print(f"  ❌ Playwright browser launch failed for {slug}: {e}", file=sys.stderr)
        return None


# ── Rating computation moved to sports-analysis: lib/logic/wta_mapper.py ──────
# The NORM dict, normalize_stat(), safe_avg(), score_or_default(), and
# compute_ratings() previously lived here. They are now in the analysis layer
# where they belong (Transform, not Extract). This scraper emits raw_stats only.


# ── Data availability metadata ────────────────────────────────────────────────

def check_charting_availability(tables: list) -> dict:
    """Return which charting sections had data for this player."""
    return {
        "hasChartingServe":  bool(get_table_career_row(tables, "Unret%", "<=3 W%")),
        "hasChartingReturn": bool(get_table_career_row(tables, "RiP%", "RetWnr%", "Slice%")),
        "hasChartingRally":  bool(get_table_career_row(tables, "RallyLen", "1-3 W%", "BH Slice%")),
        "hasChartingTactics":bool(get_table_career_row(tables, "SnV Freq", "Net Freq", "RallyAgg")),
    }


def parse_elo(soup: BeautifulSoup) -> dict:
    """
    Parse current Elo ratings from the Tour-Level Seasons table.

    The seasons table (Table 12 on player pages) has headers:
      Year | WTA Rank | Points | Elo Rank | Elo | hElo Rank | hElo | cElo Rank | cElo | gElo Rank | gElo | ...

    The 'Current' row contains live ratings for all surfaces.

    Returns dict with keys: elo, eloRank, hElo, hEloRank, cElo, cEloRank, gElo, gEloRank
    All values are integers, or None if not found.
    """
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if not first_row:
            continue
        headers = [normalize_text(c.get_text()) for c in first_row.find_all(["th", "td"])]

        # Must have Elo AND surface-specific Elo columns
        if "Elo" not in headers or "hElo" not in headers or "cElo" not in headers:
            continue

        # Find the "Current" row
        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            year_text = cells[0].get_text(strip=True)
            if "Current" not in year_text and not year_text.startswith("Current"):
                continue

            # Map headers to cell values
            data = {}
            for i, h in enumerate(headers):
                if i < len(cells):
                    val = cells[i].get_text(strip=True)
                    data[h] = val

            def safe_int(key: str) -> Optional[int]:
                v = data.get(key, "")
                try:
                    return int(v.replace(",", "")) if v else None
                except (ValueError, AttributeError):
                    return None

            return {
                "elo":      safe_int("Elo"),
                "eloRank":  safe_int("Elo Rank"),
                "hElo":     safe_int("hElo"),
                "hEloRank": safe_int("hElo Rank"),
                "cElo":     safe_int("cElo"),
                "cEloRank": safe_int("cElo Rank"),
                "gElo":     safe_int("gElo"),
                "gEloRank": safe_int("gElo Rank"),
            }

    return {
        "elo": None, "eloRank": None,
        "hElo": None, "hEloRank": None,
        "cElo": None, "cEloRank": None,
        "gElo": None, "gEloRank": None,
    }


def parse_recent_matches(soup: BeautifulSoup, player_slug: str, top_n: int = 5) -> list:
    """
    Parse the last N match results from the Recent Results table on a Tennis Abstract player page.

    The Recent Results table has headers:
      Date | Tournament | Surface | Rd | Rk | vRk | [match description] | Score | DR | A% | ...

    The match description cell (index 6) contains strings like:
      "(1)Sabalenka d. (3)Elena Rybakina [KAZ]"  → player won
      "(3)Elena Rybakina [KAZ] d. (1)Sabalenka"  → player lost

    Returns a list of dicts with: date, tournament, surface, round, opponent, score, result ('W'/'L')
    """
    matches = []

    # Find the Recent Results table — identified by headers containing 'Date', 'Tournament', 'Score'
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if not first_row:
            continue
        headers = [normalize_text(c.get_text()) for c in first_row.find_all(["th", "td"])]

        # Must have Date, Tournament, and Score columns
        if "Date" not in headers or "Tournament" not in headers or "Score" not in headers:
            continue

        date_idx        = headers.index("Date")
        tournament_idx  = headers.index("Tournament")
        surface_idx     = headers.index("Surface") if "Surface" in headers else None
        round_idx       = headers.index("Rd") if "Rd" in headers else None
        desc_idx        = 6  # match description is consistently at index 6
        score_idx       = headers.index("Score")

        rows = table.find_all("tr")[1:]  # skip header row

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) <= score_idx:
                continue

            date        = cells[date_idx].get_text(strip=True) if date_idx < len(cells) else ""
            tournament  = cells[tournament_idx].get_text(strip=True) if tournament_idx < len(cells) else ""
            surface     = cells[surface_idx].get_text(strip=True) if surface_idx and surface_idx < len(cells) else ""
            round_      = cells[round_idx].get_text(strip=True) if round_idx and round_idx < len(cells) else ""
            score       = cells[score_idx].get_text(strip=True) if score_idx < len(cells) else ""
            desc        = cells[desc_idx].get_text(strip=True) if desc_idx < len(cells) else ""

            if not date or not score:
                continue

            # Parse win/loss and opponent from description
            # Pattern: "PlayerA d. PlayerB" — the player who appears before " d. " won
            # Use player surname for matching — more robust than slug fragment
            result = "W"
            opponent = ""
            if " d. " in desc:
                parts = desc.split(" d. ")
                player_surname = player_slug.rstrip("0123456789")  # strip trailing digits (e.g. "Gasanova2")
                # Extract surname: split CamelCase slug and take last word
                # "ArynaSabalenka" -> ["Aryna", "Sabalenka"] -> "sabalenka"
                words = re.findall(r"[A-Z][a-z]*", player_surname)
                surname = words[-1].lower() if words else player_surname.lower()
                winner_part = parts[0].lower()
                result = "W" if surname in winner_part else "L"
                opponent_raw = parts[1] if result == "W" else parts[0]
                # Remove seeding e.g. "(3)" or "(Q)" from start, country code "[KAZ]" from end
                opponent = re.sub(r"^\([^)]*\)\s*", "", opponent_raw).strip()
                opponent = re.sub(r"\s*\[[A-Z]{3}\]\s*$", "", opponent).strip()
            else:
                # Walkover or retired — use full desc as opponent
                opponent = desc

            matches.append({
                "date":       date,
                "tournament": tournament,
                "surface":    surface,
                "round":      round_,
                "opponent":   opponent,
                "score":      score,
                "result":     result,
            })

            if len(matches) >= top_n:
                break

        if matches:  # found and parsed the right table
            break

    return matches


# ── Per-player scrape ─────────────────────────────────────────────────────────

def scrape_player(player: dict, debug: bool = False, page=None) -> Optional[dict]:
    slug = player["slug"]
    name = player["name"]
    print(f"  Scraping {name} ({slug})...")

    soup = fetch_player_page(slug, page=page)
    if soup is None:
        return None

    tables = soup.find_all("table")
    if not tables:
        return None

    if debug:
        print(f"  DEBUG: {len(tables)} tables found")
        for i, t in enumerate(tables):
            first_row = t.find("tr")
            if first_row:
                headers = [c.get_text(strip=True) for c in first_row.find_all(["th", "td"])]
                print(f"  Table {i:2d}: {' | '.join(headers[:8])}")

    # Extract raw stats from all table sources — no normalization, no scoring.
    # Rating computation now lives in sports-analysis: lib/logic/wta_mapper.py.
    seasons  = get_table_career_row(tables, "Hld%", "Brk%", "A%")
    we       = get_table_career_row(tables, "Wnr/Pt", "UFE/Pt", "FH Wnr/Pt")
    keypts   = get_table_career_row(tables, "BP Saved", "GP Conv")
    ch_serve = get_table_career_row(tables, "Unret%", "<=3 W%")
    ch_ret   = get_table_career_row(tables, "RiP%", "RetWnr%", "Slice%")
    ch_rally = get_table_career_row(tables, "RallyLen", "1-3 W%", "BH Slice%")
    ch_tact  = get_table_career_row(tables, "SnV Freq", "Net Freq", "RallyAgg")

    raw_stats = {
        # Seasons
        "hld_pct":      pct(seasons.get("Hld%")),
        "brk_pct":      pct(seasons.get("Brk%")),
        "ace_pct":      pct(seasons.get("A%")),
        "df_pct":       pct(seasons.get("DF%")),
        "first_in":     pct(seasons.get("1stIn")),
        "first_w":      pct(seasons.get("1st%")),
        "second_w":     pct(seasons.get("2nd%")),
        "rpw":          pct(seasons.get("RPW")),
        # Winners & Errors
        "wnr_pt":       pct(we.get("Wnr/Pt")),
        "ufe_pt":       pct(we.get("UFE/Pt")),
        "fh_wnr_pt":    pct(we.get("FH Wnr/Pt")),
        "bh_wnr_pt":    pct(we.get("BH Wnr/Pt")),
        "vs_ufe_pt":    pct(we.get("vs UFE/Pt")),
        # Key Points
        "bp_saved":     pct(keypts.get("BP Saved")),
        "gp_conv":      pct(keypts.get("GP Conv")),
        # Charting: Serve
        "unret_pct":    pct(ch_serve.get("Unret%")),
        "lt3_w":        pct(ch_serve.get("<=3 W%")),
        "rip_w_serve":  pct(ch_serve.get("RiP W%")),
        # Charting: Return
        "rip_pct":      pct(ch_ret.get("RiP%")),
        "rip_w":        pct(ch_ret.get("RiP W%")),
        "ret_wnr_pct":  pct(ch_ret.get("RetWnr%")),
        "slice_ret_pct":pct(ch_ret.get("Slice%")),
        "fhbh_ratio":   val(ch_ret.get("FH/BH")),
        # Charting: Rally
        "rally_len":    val(ch_rally.get("RallyLen")),
        "s13_w":        pct(ch_rally.get("1-3 W%")),
        "s10p_w":       pct(ch_rally.get("10+ W%")),
        "bh_slice_pct": pct(ch_rally.get("BH Slice%")),
        "fhp100":       val(ch_rally.get("FHP/100")),
        "bhp100":       val(ch_rally.get("BHP/100")),
        # Charting: Tactics
        "snv_freq":     pct(ch_tact.get("SnV Freq")),
        "net_freq":     pct(ch_tact.get("Net Freq")),
        "net_w":        pct(ch_tact.get("Net W%")),
        "drop_freq":    pct(ch_tact.get("Drop: Freq")),
        "rally_agg":    val(ch_tact.get("RallyAgg")),
        "return_agg":   val(ch_tact.get("ReturnAgg")),
    }

    availability = check_charting_availability(tables)
    recent_matches = parse_recent_matches(soup, slug, top_n=5)
    elo = parse_elo(soup)

    if debug:
        print(f"  DEBUG: Recent matches found: {len(recent_matches)}")
        for m in recent_matches:
            print(f"    {m['result']} {m['date']} {m['tournament']} {m['round']} vs {m['opponent']} {m['score']}")
        print(f"  DEBUG: Elo ratings: {elo}")
        populated = {k: v for k, v in raw_stats.items() if v is not None}
        print(f"  DEBUG: raw_stats populated: {len(populated)}/{len(raw_stats)} fields")

    # Derive nationality emoji from country code
    country = player.get("country", "")
    emoji = country_to_flag(country)

    return {
        "name":             name,
        "slug":             slug,
        "country":          country,
        "emoji":            emoji,
        "rank":             player["rank"],
        "lastUpdated":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "raw_stats":        raw_stats,
        "elo":              elo,
        "recentMatches":    recent_matches,
        "dataAvailability": availability,
    }


def country_to_flag(code: str) -> str:
    """Convert ISO 3166-1 alpha-3 country code to flag emoji (best effort)."""
    mapping = {
        "BLR": "🇧🇾", "POL": "🇵🇱", "KAZ": "🇰🇿", "USA": "🇺🇸", "ITA": "🇮🇹",
        "RUS": "🇷🇺", "UKR": "🇺🇦", "CAN": "🇨🇦", "CZE": "🇨🇿", "CHN": "🇨🇳",
        "GBR": "🇬🇧", "GER": "🇩🇪", "FRA": "🇫🇷", "ESP": "🇪🇸", "AUS": "🇦🇺",
        "BEL": "🇧🇪", "TUN": "🇹🇳", "GRE": "🇬🇷", "LAT": "🇱🇻", "CRO": "🇭🇷",
        "DEN": "🇩🇰", "BRA": "🇧🇷", "ARG": "🇦🇷", "JPN": "🇯🇵", "NED": "🇳🇱",
        "SUI": "🇨🇭", "SVK": "🇸🇰", "ROU": "🇷🇴", "BUL": "🇧🇬", "MEX": "🇲🇽",
        "NZL": "🇳🇿", "EGY": "🇪🇬", "HUN": "🇭🇺", "COL": "🇨🇴", "ARM": "🇦🇲",
    }
    return mapping.get(code, "🎾")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="WTA Tennis Abstract Scraper")
    parser.add_argument("slug", nargs="?", help="Single player slug (e.g. IgaSwiatek)")
    parser.add_argument("--top", type=int, default=int(os.environ.get("TOP_N", 0)),
                        help="Number of top-ranked players to scrape")
    args = parser.parse_args()

    out_path = os.environ.get("OUTPUT_PATH", "players.json")

    # ── Determine who to scrape ───────────────────────────────────────────────
    if args.slug:
        player = next((p for p in FALLBACK_PLAYERS if p["slug"] == args.slug), None)
        if not player:
            player = {"rank": 0, "name": args.slug, "slug": args.slug, "country": ""}
        players_to_scrape = [player]
    elif args.top > 0:
        players_to_scrape = fetch_rankings(top_n=args.top)
    else:
        print("❌ No slug or --top provided.", file=sys.stderr)
        sys.exit(1)

    results = []
    errors  = []
    debug   = len(players_to_scrape) == 1

    print(f"🎾 Scraping {len(players_to_scrape)} WTA players from Tennis Abstract...")

    # ── Browser session management ───────────────────────────────────────────
    # Cloudflare tracks session state at the context level — sharing any browser
    # context across players causes blocks after the first successful load.
    # Only a fully fresh browser per player reliably bypasses this.
    # Tradeoff: ~15-20s per player, ~60-80 min for 250 players.

    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    def scrape_with_fresh_browser(player, debug):
        """Launch a fully isolated browser, scrape one player, close immediately."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            Stealth().apply_stealth_sync(page)
            try:
                return scrape_player(player, debug=debug, page=page)
            finally:
                browser.close()

    for i, player in enumerate(players_to_scrape):
        try:
            result = scrape_with_fresh_browser(player, debug)
            if result:
                results.append(result)
                print(f"  ✅ {player['name']}")
            else:
                errors.append(player["name"])
        except Exception as e:
            print(f"  ❌ {player['name']}: {e}", file=sys.stderr)
            errors.append(player["name"])

        # Polite delay between requests
        if i < len(players_to_scrape) - 1:
            time.sleep(random.uniform(2.0, 4.0))

    # ── Write output — framework wrapper handles GCS/storage ─────────────────
    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "playerCount": len(results),
        "players":     results,
        "errors":      errors,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Done. {len(results)} scraped, {len(errors)} errors → {out_path}")
    if errors:
        print(f"❌ Failed: {', '.join(errors)}")

    # ── Exit code discipline: signal failure to wrapper ───────────────────────
    if args.slug and not results:
        print(f"❌ No data returned for slug: {args.slug}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
