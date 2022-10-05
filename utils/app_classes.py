from dataclasses import dataclass
import datetime

@dataclass
class Fixture:
    fixture_id: int
    home_team_id : int
    away_team_id : int
    date: datetime.datetime

@dataclass
class PlayerStat:
    player_id: int
    player_name: str
    rating: float
    date: datetime.datetime