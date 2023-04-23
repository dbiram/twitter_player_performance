from config import Config
import pymongo
import pandas as pd

class DB_mongodb:
    @staticmethod
    def mongodb_append_field(df, field):
        client = pymongo.MongoClient(f"mongodb://{Config.MONGODB_host}:{Config.MONGODB_port}/")
        db = client[Config.MONGODB_dbname]
        collection = db[Config.MONGODB_collectionname]
        player_ids = collection.distinct("player_id")

        for player in player_ids:
            if field == 'tweets':
                df.day = df.day.astype(str)
            dfp = df[df.player_id == player].loc[:, ~df.columns.isin(['player_id', 'player_name'])]
            dfp_dict = dfp.to_dict('records')
            if len(dfp_dict)>0:
                query = {"player_id": player}
                update = {"$push": {field: {'$each': dfp_dict}}}
                result = collection.update_many(query, update)
        client.close()

