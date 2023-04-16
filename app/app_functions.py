from bundesliga_api.BundesligaAPI import BundesligaAPI
from twitter.TwitterPlayerSearch import TwitterPlayerSearch
from twitter.SentimentAnalysis import SentimentAnalysis
from data import AppData
from utils import Fixture, PlayerStat, PlayerTweet

from datetime import datetime
import pandas as pd
import csv
import hdfs
from dataclasses import asdict
import fastavro

import logging

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logging.basicConfig(level=logging.INFO)
rootLogger = logging.getLogger()
logPath = "log"
fileName = "jobrunner"
fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName))
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)


class AppFunctions:
    path = str
    data = AppData
    hdfs_client = hdfs.Client

    def __init__(self, hdfs_path=None, path="../data/"):
        rootLogger.info("Starting a new App with destination path = "+str(path))
        self.path = path
        self.data = AppData(path)
        if hdfs_path:
            self.hdfs_client = hdfs.InsecureClient(hdfs_path)
            rootLogger.info("Connected to HDFS master data : " + str(hdfs_path))
        else:
            self.hdfs_client = None

    def get_relevant_matches(self, date):
        teams = self.data.teams_mapping
        rootLogger.info("Getting relevant Fixtures for date = " + date.strftime("%m/%d/%Y, %H:%M:%S"))
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
        rootLogger.info("Getting relevant Players rates in fixture = "+str(fixture.home_team_id)+" Vs "+str(fixture.away_team_id))
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
        rootLogger.info("Getting tweets for relevant Players for date = " + date.strftime("%m/%d/%Y, %H:%M:%S"))
        for player, player_id in zip(self.data.players.player_name, self.data.players.player_id):
            tweets_json = TwitterPlayerSearch.search_particular_date(player, date, max_tweets).json()
            if tweets_json["meta"]["result_count"] > 0:
                tweets = tweets_json["data"]
                for tweet in tweets:
                    creation_date = datetime.strptime(tweet["created_at"][:19], "%Y-%m-%dT%H:%M:%S")
                    relevant.append(PlayerTweet(player_id, player, tweet["text"], creation_date, date))
        return relevant

    def add_list_to_avro(self, type, list, batch_id):
        rootLogger.info("Sending Avro file to HDFS ....")
        rootLogger.info("File Type = "+type)
        rootLogger.info("Batch id = " + batch_id)
        rootLogger.info("Number of elements = " + str(len(list)))
        if type == "fixture":
            print(list)
            schema = {
                "type": "record",
                "namespace": "twitter_player_performance",
                "name": "Fixture",
                "doc": "Relevant fixtures (contains tracked teams)",
                "fields": [
                    {"name": "id$", "type": "long"},
                    {"name": "fixture_id", "type": "int"},
                    {"name": "home_team_id", "type": "int"},
                    {"name": "away_team_id", "type": "int"},
                    {"name": "date", "type": {"type": "int", "logicalType": "time-millis"}}
                ]
            }
        elif type == "player":
            print(list)
            schema = {
                "type": "record",
                "namespace": "twitter_player_performance",
                "name": "Player",
                "doc": "Relevant players ratings during fixtures (contains tracked players)",
                "fields": [
                    {"name": "id$", "type": "long"},
                    {"name": "player_id", "type": "int"},
                    {"name": "player_name", "type": "string"},
                    {"name": "rating", "type": "float"},
                    {"name": "date", "type": {"type": "int", "logicalType": "time-millis"}}
                ]
            }
        elif type == "tweet":
            schema = {
                "type": "record",
                "namespace": "twitter_player_performance",
                "name": "Tweet",
                "doc": "Tweets fetched about relevant players (contains tracked players)",
                "fields": [
                    {"name": "id$", "type": "long"},
                    {"name": "player_id", "type": "int"},
                    {"name": "player_name", "type": "string"},
                    {"name": "tweet_body", "type": "string"},
                    {"name": "created_at", "type": {"type": "int", "logicalType": "time-millis"}},
                    {"name": "date", "type": {"type": "int", "logicalType": "time-millis"}}
                ]
            }

        def add_id(dict_arg):
            dict_arg["id$"] = int(datetime.now().timestamp() * 1000000)
            dict_arg["date"] = int(datetime.timestamp(dict_arg["date"]))
            if "created_at" in dict_arg:
                dict_arg["created_at"] = int(datetime.timestamp(dict_arg["created_at"]))
            return dict_arg

        data = [add_id(asdict(item)) for item in list]
        with self.hdfs_client.write("/data/master_data/"+type+"/all_" + type + "s_"+batch_id+".avro", overwrite=True) as avro_file:
            fastavro.writer(avro_file, schema, data)

    def append_master_data(self, date):
        batch_id = date.strftime('%s')
        matches = self.get_relevant_matches(date)
        if self.hdfs_client and (len(matches) > 0):
            self.add_list_to_avro("fixture", matches, batch_id)
        if len(matches) > 0:
            for fixture in matches:

                with open(self.path + 'master_data/all_fixtures.csv', 'a+', newline='', encoding="utf-8") as write_obj:
                    csv_writer = csv.writer(write_obj, delimiter=';')
                    csv_writer.writerow(
                        [int(datetime.now().timestamp() * 1000), fixture.fixture_id, fixture.home_team_id,
                         fixture.away_team_id, fixture.date])
                players = self.get_relevant_player_stats(fixture)
                if self.hdfs_client and (len(players) > 0):
                    self.add_list_to_avro("player", players, batch_id)
                if len(players) > 0:
                    for player in players:
                        with open(self.path + 'master_data/all_player_rates.csv', 'a+', newline='',
                                  encoding="utf-8") as write_obj:
                            csv_writer = csv.writer(write_obj, delimiter=';')
                            csv_writer.writerow(
                                [int(datetime.now().timestamp() * 1000), player.player_id, player.player_name,
                                 player.rating, player.date])
        tweets = self.get_relevant_tweets(date)
        if self.hdfs_client and (len(tweets) > 0):
            self.add_list_to_avro("tweet", tweets, batch_id)
        tweets_df = {"id$": [], "player_id": [], "player_name": [], "tweet_body": [], "created_at": [], "date": []}
        if len(tweets) > 0:
            for tweet in tweets:
                tweet_id = int(datetime.now().timestamp() * 1000)
                tweets_df["id$"].append(tweet_id)
                tweets_df["player_id"].append(tweet.player_id)
                tweets_df["player_name"].append(tweet.player_name)
                tweets_df["tweet_body"].append(tweet.tweet_body)
                tweets_df["created_at"].append(str(tweet.created_at))
                tweets_df["date"].append(str(tweet.date))
                with open(self.path + 'master_data/all_tweets.csv', 'a+', newline='', encoding="utf-8") as write_obj:
                    csv_writer = csv.writer(write_obj, delimiter=';')
                    csv_writer.writerow(
                        [tweet_id, tweet.player_id, tweet.player_name, tweet.tweet_body, tweet.created_at, tweet.date])
        tweets_pd_df = pd.DataFrame(tweets_df)
        if tweets_pd_df.shape[0] > 0:
            analysis = SentimentAnalysis(tweets_pd_df)
            analysis.tweets_analysed.to_csv(self.path + 'analysed_data/all_analysed_tweets.csv', mode='a', header=False,
                                            index=False, sep=';')
