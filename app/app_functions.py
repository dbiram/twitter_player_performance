from bundesliga_api.BundesligaAPI import BundesligaAPI
from twitter.TwitterPlayerSearch import TwitterPlayerSearch
from data import AppData
from utils import Fixture, PlayerStat, PlayerTweet

from datetime import datetime
import csv


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

    def get_relevant_tweets(self, date, max_tweets = 100):
        relevant = []
        for player, player_id in zip(self.data.players.player_name, self.data.players.player_id):
            tweets_json = TwitterPlayerSearch.search_particular_date(player, date, max_tweets).json()
            if tweets_json["meta"]["result_count"] > 0:
                tweets = tweets_json["data"]
                for tweet in tweets:
                    creation_date = datetime.strptime(tweet["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
                    relevant.append(PlayerTweet(player_id, player, tweet["text"], creation_date, date))
        return relevant

    def append_master_data(self, date):

        matches = self.get_relevant_matches(date)
        if len(matches)>0:
            for fixture in matches:

                with open(self.path+'master_data/all_fixtures.csv', 'a+', newline='', encoding="utf-8") as write_obj:
                    csv_writer = csv.writer(write_obj, delimiter=';')
                    csv_writer.writerow([int(datetime.now().timestamp() * 1000), fixture.fixture_id, fixture.home_team_id, fixture.away_team_id, fixture.date])
                players = self.get_relevant_player_stats(fixture)
                if len(players)>0:
                    for player in players:

                        with open(self.path + 'master_data/all_player_rates.csv', 'a+', newline='', encoding="utf-8") as write_obj:
                            csv_writer = csv.writer(write_obj, delimiter=';')
                            csv_writer.writerow([int(datetime.now().timestamp() * 1000), player.player_id, player.player_name, player.rating, player.date])
        tweets = self.get_relevant_tweets(date)
        if len(tweets)>0:
            for tweet in tweets:
                with open(self.path + 'master_data/all_tweets.csv', 'a+', newline='', encoding="utf-8") as write_obj:
                    csv_writer = csv.writer(write_obj, delimiter=';')
                    csv_writer.writerow([int(datetime.now().timestamp() * 1000), tweet.player_id, tweet.player_name, tweet.tweet_body, tweet.date])


