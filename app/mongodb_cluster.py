from dataclasses import dataclass
from datetime import datetime
from pymongo import MongoClient
from app.config import MongoDbConfig
from twitter_api import const


@dataclass
class MongoDbCluster:
    _mongo_client: MongoClient

    def __init__(self, mongo_db: MongoDbConfig):
        self._mongo_client = MongoClient(
            f'mongodb+srv://{mongo_db.username}:{mongo_db.password}@{mongo_db.host}')

    def get_tweet_created_at(self, collection, sort: int):
        try:
            result = list(self._mongo_client.data_lake[collection]
                          .find({}, {"_id": 0, "created_at": 1})
                          .sort("created_at", sort).limit(1))
            if len(result) != 0 and "created_at" in result[0]:
                return datetime.strptime(result[0]["created_at"], const.iso_time_format)
        except Exception as ex:
            print(ex)

    def save_users_followers(self, users_followers: list):
        self._save_documents("data_lake", "users_followers", users_followers)

    def save_includes_users(self, includes_users: list):
        self._save_documents("data_lake", "includes_users", includes_users)

    def save_include_tweets(self, includes_tweets: list):
        self._save_documents("data_lake", "includes_tweets", includes_tweets)

    def save_search_tweets(self, search_tweets: list):
        self._save_documents("data_lake", "search_tweets", search_tweets)

    def save_user_tweets(self, user_tweets: list):
        self._save_documents("data_lake", "user_tweets", user_tweets)

    def _save_documents(self, database_name: str, collection_name: str, documents: list):
        try:
            if len(documents) > 0:
                collection = self._mongo_client[database_name][collection_name]
                collection.delete_many({'_id': {"$in": [document["_id"] for document in documents]}})
                collection.insert_many(documents)
                print("... save {} {} in {}".format(len(documents), collection_name, database_name))
        except Exception as ex:
            print(ex)
