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

@dataclass
class PlayerTweet:
    player_id: int
    player_name: str
    tweet_body: str
    created_at: datetime.datetime
    date: datetime.datetime

@dataclass
class PlayerTweetAnalysis:
    tweet_id: int
    player_id: int
    day: str
    analysis: int
