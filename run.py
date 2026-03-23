import argparse
import yaml
import logging
from registry import SCRAPER_REGISTRY
from resolvers.cbb import CBBResolver
from resolvers.tennis import TennisResolver
from base.runner import ScraperRunner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    parser = argparse.ArgumentParser(description="Sports Data Scraper CLI")
    parser.add_argument("--source", help="Name of specific scraper to run")
    parser.add_argument("--sport",  help="Group of scrapers to run (cbb or tennis)")
    parser.add_argument("--force",   action="store_true", help="Ignore content-hash skip")
    parser.add_argument("--dry-run", action="store_true", help="No GCS write")
    args = parser.parse_args()

    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    resolvers = {
        "cbb":    CBBResolver(),
        "tennis": TennisResolver(),
    }

    sources_to_run = []
    if args.source:
        sources_to_run = [args.source]
    elif args.sport:
        sources_to_run = config["groups"].get(args.sport, [])
    else:
        print("Please provide --source or --sport.")
        return

    for name in sources_to_run:
        s_conf = config["scrapers"].get(name)
        if not s_conf or not s_conf.get("enabled"):
            logging.warning(f"Scraper '{name}' is disabled or not found in config.")
            continue

        scraper_cls = SCRAPER_REGISTRY.get(name)
        if not scraper_cls:
            logging.error(f"Scraper class for '{name}' not found in registry.")
            continue

        resolver    = resolvers.get(s_conf["sport"])
        scraper     = scraper_cls(resolver=resolver, config=s_conf)
        runner      = ScraperRunner(scraper, s_conf["bucket"])

        runner.run(force=args.force, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
