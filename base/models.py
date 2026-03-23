from datetime import datetime
from typing import Optional, List, Literal
from pydantic import BaseModel, Field

class CBBGame(BaseModel):
    espn_id: str
    date: str
    state: Literal["pre", "in", "post"]
    completed: bool
    t1_name: str
    t1_score: Optional[int] = None
    t1_winner: bool
    t2_name: str
    t2_score: Optional[int] = None
    t2_winner: bool
    source: str = "espn"
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

class OddsSnapshot(BaseModel):
    team: str
    spread: str
    moneyline: str
    ou: str
    has_lines: bool
    game_date: str
    source: str = "action_network"
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

class TennisMatch(BaseModel):
    match_id: str
    tournament: str
    round: Optional[str] = None
    date: str
    status: Literal["scheduled", "live", "finished"]
    p1_name: str
    p1_sets_won: Optional[int] = 0
    p2_name: str
    p2_sets_won: Optional[int] = 0
    set_scores: List[dict] = []
    winner: Optional[str] = None
    source: str = "sofascore"
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

class TennisPlayer(BaseModel):
    name: str
    slug: str
    elo_overall: Optional[float]
    source: str = "tennis_abstract"
    fetched_at: datetime = Field(default_factory=datetime.utcnow)