import pandas as pd


class AppData:
    sample_path = str
    teams_mapping = pd.DataFrame
    players_mapping = pd.DataFrame
    players = pd.DataFrame

    def __init__(self, path="samples/"):
        self.sample_path = path
        self.teams_mapping = pd.read_csv(path + "teams_mapping.csv", sep=";")
        self.players_mapping = pd.read_csv(path + "mapping_players.csv", sep=";")
        self.players = pd.read_csv(path + "players_sample.csv", sep=";")
