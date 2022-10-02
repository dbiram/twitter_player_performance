from bundesliga_api.BundesligaAPI import BundesligaAPI
import pandas as pd

from utils import Fixture


def get_relevant_matches(date):
    teams = pd.read_csv("data/samples/teams_mapping.csv", sep = ";")
    matches = BundesligaAPI.get_list_matches(date).json()["response"]
    relevant = []

    for match in matches:
        if (match["teams"]["home"]["id"] in teams.football_api_team_id.to_list()) or (
                match["teams"]["away"]["id"] in teams.football_api_team_id.to_list()):
            relevant.append(Fixture(match["fixture"]["id"], match["teams"]["home"]["id"], match["teams"]["away"]["id"]))

    return relevant
