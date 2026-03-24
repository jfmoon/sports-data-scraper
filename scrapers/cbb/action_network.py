import requests
from datetime import datetime, timezone
from base.scraper import BaseScraper
from base.models import OddsSnapshot
from base.storage import StorageManager
from scrapers.cbb.names import to_canonical


class ActionNetworkScraper(BaseScraper):
    def fetch(self):
        d_str   = datetime.now().strftime("%Y%m%d")
        url     = f"https://api.actionnetwork.com/web/v2/scoreboard/ncaab?bookIds=68&date={d_str}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res     = requests.get(url, headers=headers, timeout=15)
        res.raise_for_status()
        return res.json()

    def content_key(self, raw):
        return raw.get("games", [])

    def parse(self, raw):
        parsed = []
        for g in raw.get("games", []):
            away_id = g.get("away_team_id")
            home_id = g.get("home_team_id")

            dk_mkt = None
            for v in g.get("markets", {}).values():
                if v.get("event", {}).get("spread"):
                    dk_mkt = v.get("event")
                    break

            def fmt_val(v, ml=False):
                if v is None: return "" if ml else "TBD"
                return f"+{v}" if v > 0 else str(v)

            for team_type in ["away", "home"]:
                tid    = away_id if team_type == "away" else home_id
                t_raw  = next((t for t in g["teams"] if t["id"] == tid), {})
                t_name = to_canonical(self.resolver.resolve(t_raw.get("full_name", "")))

                row = {
                    "team": t_name, "spread": "TBD", "moneyline": "",
                    "ou": "TBD", "has_lines": False,
                    "game_date": g["start_time"][:10]
                }

                if dk_mkt:
                    spr = next((s["value"] for s in dk_mkt.get("spread", [])    if s["team_id"] == tid), None)
                    ml  = next((m["odds"]  for m in dk_mkt.get("moneyline", []) if m["team_id"] == tid), None)
                    ou  = next((t["value"] for t in dk_mkt.get("total", [])     if t["side"] == "over"), None)
                    row.update({
                        "spread": fmt_val(spr), "moneyline": fmt_val(ml, True),
                        "ou": str(ou) if ou else "TBD", "has_lines": True
                    })
                parsed.append(row)
        return parsed

    def validate(self, records):
        return [OddsSnapshot(**r) for r in records]

    def upsert(self, records):
        storage  = StorageManager(self.config["bucket"])
        odds_map = {
            r.team: {
                "s": r.spread, "ml": r.moneyline,
                "ou": r.ou, "ok": r.has_lines, "date": r.game_date
            } for r in records
        }
        payload = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "source":  "Action Network / DraftKings",
            "odds":    odds_map
        }
        storage.write_json(self.config["gcs_object"], payload)
