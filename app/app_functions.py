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

from postgres import DB_postgres
from mongodb import DB_mongodb

fastavro.read.LOGICAL_READERS["int-time-millis"] = lambda a,b,c:a

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
    mongoDB_mapping = {'tweet': 'tweets', 'player': 'ratings'}

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
            players = []
            for fixture in matches:
                players.extend(self.get_relevant_player_stats(fixture))
            if self.hdfs_client and (len(players) > 0):
                self.add_list_to_avro("player", players, batch_id)
        tweets = self.get_relevant_tweets(date)
        if self.hdfs_client and (len(tweets) > 0):
            self.add_list_to_avro("tweet", tweets, batch_id)

    def read_avro_from_hdfs(self, path):
        records = []
        with self.hdfs_client.read(path) as avro_file:
            avro_reader = fastavro.reader(avro_file)
            for record in avro_reader:
                records.append(record)
        return pd.DataFrame(records)

    def append_analysed_data(self):
        rootLogger.info("Reading Avro files from HDFS ....")
        def convert_date(int_date):
            return datetime.fromtimestamp(int_date).strftime("%Y-%m-%d")

        types = ['fixture', 'player', 'tweet']
        for t in types:
            p = "/data/master_data/"+t
            p_processed = "/data/processed/"+t
            to_be_processed = self.hdfs_client.list(p)
            rootLogger.info("Files to be processed = "+str(to_be_processed))
            if len(to_be_processed) > 0:
                for f in to_be_processed:
                    df = self.read_avro_from_hdfs(p+"/"+f)
                    df.date = pd.to_datetime(df['date'], unit='s')
                    if t == 'tweet':
                        df["created_at"] = df['created_at'].apply(convert_date)
                        if df.shape[0] > 0:
                            analysis = SentimentAnalysis(df)
                            df = analysis.tweets_analysed
                    if df.shape[0] > 0:
                        rootLogger.info("Writing analysed data in Postgres ....")
                        #df.to_csv('data/analysed_data/all_'+t+'.csv', mode='a', header=False, index=False,sep=';')
                        DB_postgres.postgres_append_table(df,f"all_{t}s")
                        rootLogger.info(f"Analysed data for {t}s are appended in Postgres table all_{t}s")

                        if t in ['tweet', 'player']:
                            rootLogger.info("Writing analysed data in MongoDB ....")
                            DB_mongodb.mongodb_append_field(df, self.mongoDB_mapping[t])
                            rootLogger.info(f"Analysed data for {t}s are appended in MongoDB collection field {self.mongoDB_mapping[t]}")

                    rootLogger.info("Moving file = "+f+" to processed ....")
                    self.hdfs_client.rename(p+"/"+f, p_processed+"/"+f)