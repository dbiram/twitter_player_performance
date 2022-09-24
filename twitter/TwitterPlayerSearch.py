from config import Config
import tweepy
import requests
import json


class TwitterPlayerSearch:
    client = tweepy.Client(bearer_token=Config.BEARER_TOKEN,
                           consumer_key=Config.API_KEY,
                           consumer_secret=Config.API_KEY_SECRET,
                           access_token=Config.ACCESS_TOKEN,
                           access_token_secret=Config.ACCESS_TOKEN_SECRET,
                           return_type=requests.Response,
                           wait_on_rate_limit=True)

    @staticmethod
    def search(player_name, player_account, team_account, max_tweets=100):
        query = f"""{player_name} lang:en -is:retweet OR {player_name} lang:de -is:retweet"""
        try:

            tweets = TwitterPlayerSearch.client.search_recent_tweets(query=query,
                                                      tweet_fields=['author_id', 'created_at'],
                                                      max_results=max_tweets)
            print("-- Successfully connected to Twitter API --")
            return tweets
        except Exception as e:
            print("-- Couldn't connect to Twitter API --")
            print(e)

    @staticmethod
    def jprint(obj):
        # create a formatted string of the Python JSON object
        text = json.dumps(obj, sort_keys=True, indent=4)
        print(text)
