from abc import ABC, abstractmethod
from typing import Any, List
from pydantic import BaseModel

class BaseScraper(ABC):
    def __init__(self, resolver=None, config: dict = None):
        self.resolver = resolver
        self.config = config or {}
        self.source_name = self.config.get("name", "unknown")

    @abstractmethod
    def fetch(self) -> Any:
        """Fetch raw data from source."""
        pass

    @abstractmethod
    def content_key(self, raw: Any) -> Any:
        """Return the data slice for hashing (exclude envelope/timestamps)."""
        pass

    @abstractmethod
    def parse(self, raw: Any) -> List[dict]:
        """Parse raw response into list of normalized dicts."""
        pass

    @abstractmethod
    def validate(self, records: List[dict]) -> List[BaseModel]:
        """Run Pydantic validation."""
        pass

    @abstractmethod
    def upsert(self, records: List[BaseModel]) -> None:
        """Write validated records to GCS."""
        pass