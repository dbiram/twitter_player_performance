import pandas as pd
from langdetect import detect
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class SentimentAnalysis:
    tweets = pd.DataFrame
    tweets_en = pd.DataFrame
    applied_df = pd.DataFrame
    tweets_analysed = pd.DataFrame

    def __init__(self, tweets):
        self.tweets = tweets
        self.get_tweet_analysis()

    def get_language(self, row):
        """ Function used in df.apply """
        try:
            return detect(row["tweet_body"])
        except Exception as e:
            return 'en'

    def get_analysis_score(self, row):
        """ Function used in df.apply """
        score = SentimentIntensityAnalyzer().polarity_scores(row['tweet_body'])
        return score['neg'], score['neu'], score['pos']

    def get_analysis_cat(self, row):
        """ Function used in df.apply """
        if row['negative'] > row['positive']:
            return -1
        elif row['negative'] < row['positive']:
            return 1
        else:
            return 0

    def get_tweet_analysis(self):
        self.tweets["language"] = self.tweets.apply(lambda row: self.get_language(row), axis=1)
        self.tweets_en = self.tweets[self.tweets.language == 'en']
        self.applied_df = self.tweets_en.apply(lambda row: self.get_analysis_score(row), axis='columns', result_type='expand')
        self.applied_df.columns = ["negative", "neutral", "positive"]
        self.tweets_analysed = pd.concat([self.tweets_en, self.applied_df], axis='columns')
        self.tweets_analysed['category'] = self.tweets_analysed.apply(lambda row: self.get_analysis_cat(row), axis='columns')
        self.tweets_analysed['day'] = self.tweets_analysed.created_at.apply(lambda x: x[:10])
        self.tweets_analysed = self.tweets_analysed[['player_id','id$','day','category']]