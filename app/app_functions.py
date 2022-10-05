from bundesliga_api.BundesligaAPI import BundesligaAPI
from data import AppData
from utils import Fixture, PlayerStat


class AppFunctions:
    path = str
    data = AppData

    def __init__(self, path="../data/"):
        self.path = path
        self.data = AppData(path)

    def get_relevant_matches(self, date):
        teams = self.data.teams_mapping
        matches = BundesligaAPI.get_list_matches(date).json()["response"]
        relevant = []

        for match in matches:
            if (match["teams"]["home"]["id"] in teams.football_api_team_id.to_list()) or (
                    match["teams"]["away"]["id"] in teams.football_api_team_id.to_list()):
                relevant.append(
                    Fixture(match["fixture"]["id"], match["teams"]["home"]["id"], match["teams"]["away"]["id"], date))

        return relevant

    def get_relevant_player_stats(self, fixture):
        stats = BundesligaAPI.player_stats_from_fixture_id(fixture.fixture_id).json()["response"]

        relevant = []

        for s in stats:
            for player in s["players"]:
                id_api = player["player"]["id"]
                if id_api in self.data.players_mapping.football_api_player_id.to_list():
                    id_local = self.data.players_mapping[
                        self.data.players_mapping.football_api_player_id == id_api].player_id.values[0]
                    player_name = self.data.players[self.data.players.player_id == id_local].player_name.values[0]
                    rating = player["statistics"][0]["games"]["rating"]
                    if rating:
                        relevant.append(PlayerStat(id_local, player_name, float(rating), fixture.date))
                    else:
                        relevant.append(PlayerStat(id_local, player_name, 0, fixture.date))
        return relevant
