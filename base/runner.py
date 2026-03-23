import json
import hashlib
import logging
from .state import StateManager
from .storage import StorageManager


class ScraperRunner:
    def __init__(self, scraper, bucket_name: str):
        self.scraper    = scraper
        self.state_mgr  = StateManager(bucket_name)
        self.storage_mgr = StorageManager(bucket_name)
        self.logger     = logging.getLogger(scraper.source_name)

    def run(self, force=False, dry_run=False):
        source = self.scraper.source_name
        state  = self.state_mgr.get_state(source)

        try:
            raw_data = self.scraper.fetch()

            # Stash raw path/mode on scraper so upsert() can upload the file
            if isinstance(raw_data, dict) and "path" in raw_data:
                self.scraper._raw_path = raw_data.get("path")
                self.scraper._raw_mode = raw_data.get("mode", "ratings")

            # Persist raw to local disk + GCS
            self.storage_mgr.persist_raw(source, raw_data)

            # Change detection — skip if content unchanged
            data_slice  = self.scraper.content_key(raw_data)
            content_str = json.dumps(data_slice, sort_keys=True)
            new_hash    = hashlib.md5(content_str.encode()).hexdigest()

            if not force and new_hash == state.get("last_content_hash"):
                self.logger.info(f"Skipping {source}: content unchanged.")
                return

            parsed_dicts    = self.scraper.parse(raw_data)
            validated_models = self.scraper.validate(parsed_dicts)

            if not dry_run:
                self.scraper.upsert(validated_models)
                self.state_mgr.update_success(source, new_hash)
                self.logger.info(f"Success: {source} — {len(validated_models)} records.")
            else:
                self.logger.info(f"DRY RUN: {source} — {len(validated_models)} records.")

        except Exception as e:
            self.logger.error(f"FAILED {source}: {str(e)}", exc_info=True)
