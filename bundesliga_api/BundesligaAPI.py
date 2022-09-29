import requests
import json
from datetime import datetime
from config import Config


class BundesligaAPI:
    bundesliga_id = 78
    @staticmethod
    def get_list_matchs(date):
        url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/players"

        querystring = {"date" : datetime.strftime(date, "%Y-%m-%d"),
                       "league" : str(BundesligaAPI.bundesliga_id),
                       "season" : "2022"}

        headers = {
            "X-RapidAPI-Key": Config.X_RapidAPI_Key,
            "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        return response

    @staticmethod
    def jprint(response):
        # create a formatted string of the Python JSON object
        text = json.dumps(response.json()["response"], sort_keys=True, indent=4)
        print(text)
