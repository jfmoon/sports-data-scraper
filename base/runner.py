import json
import hashlib
import logging
from .state import StateManager
from .storage import StorageManager

class ScraperRunner:
    def __init__(self, scraper, bucket_name: str):
        self.scraper = scraper
        self.state_mgr = StateManager(bucket_name)
        self.storage_mgr = StorageManager(bucket_name)
        self.logger = logging.getLogger(scraper.source_name)

    def run(self, force=False, dry_run=False):
        source = self.scraper.source_name
        state = self.state_mgr.get_state(source)

        try:
            raw_data = self.scraper.fetch()
            self.storage_mgr.persist_raw(source, raw_data)

            # Change Detection Logic (MANDATORY sort_keys=True)
            data_slice = self.scraper.content_key(raw_data)
            content_str = json.dumps(data_slice, sort_keys=True)
            new_hash = hashlib.md5(content_str.encode()).hexdigest()

            if not force and new_hash == state.get("last_content_hash"):
                self.logger.info(f"Skipping {source}: Content unchanged.")
                return

            parsed_dicts = self.scraper.parse(raw_data)
            validated_models = self.scraper.validate(parsed_dicts)

            if not dry_run:
                self.scraper.upsert(validated_models)
                self.state_mgr.update_success(source, new_hash)
                self.logger.info(f"Success: {source} updated with {len(validated_models)} records.")
            else:
                self.logger.info(f"DRY RUN: Found {len(validated_models)} records.")

        except Exception as e:
            self.logger.error(f"FAILED {source}: {str(e)}")