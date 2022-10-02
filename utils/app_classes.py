from dataclasses import dataclass

@dataclass
class Fixture:
    fixture_id: int
    home_team_id : int
    away_team_id : int