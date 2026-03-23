# Sports Data Operator (v4)

Modular framework for scraping NCAA and WTA data into GCS.

## Usage

### Run CBB Score Scraper
python run.py --source espn

### Run all Tennis Scrapers
python run.py --sport tennis

### Force a run (ignore content-hash check)
python run.py --source sofascore --force

### Replay a parse from local data
python run.py --source espn --dry-run