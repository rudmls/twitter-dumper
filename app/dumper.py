from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from pymongo import MongoClient, DESCENDING

from app.config import Config
from app.twitter_client import TwitterClient


class Database:
    data_lake: str = "data-lake"
    config: str = "config"


class Collection:
    user_followers: str = "users_followers"
    user_tweets: str = "user_tweets"
    search_tweets: str = "search_tweets"
    fetch_infos: str = "fetch_infos"


@dataclass
class Dumper:
    mongo_client: MongoClient
    config: Config
    twitter_client: TwitterClient

    def __init__(self, config: Config):
        self.twitter_client = TwitterClient(config.twitter.bearer_token)
        self.config = config
        self.mongo_client = MongoClient(
            f'mongodb+srv://{config.mongo_db.username}:{config.mongo_db.password}@{config.mongo_db.host}')

    def _user_tweets_query(self):
        username = self.config.dumper.username
        return f"from:{username} -is:retweet"

    def _search_tweets_query(self):
        username = self.config.dumper.username
        return f"@{username} -from:{username} -is:retweet"

    def start(self):
        try:
            # last_fetch_info = self.mongo_client[Database.config][Collection.fetch_infos]\
            #     .find_one(sort=[("fetchDate", DESCENDING)])
            last_fetch_info = None
            if not last_fetch_info:
                print('[ full dump ]')
                self._full_dump()
            else:
                print('[ light dump ]')
                self._light_dump(last_fetch_info)
        except Exception as ex:
            print(ex)

    def _full_dump(self):
        username = self.config.dumper.username
        start_time = self.config.dumper.tweet.start_time
        print(f"...get {username} user info")
        user = self.twitter_client.get_user(username=username)
        print(f"...get {username} followers")
        users_followers = self.twitter_client.get_users_followers(user["id"], 3000)
        print(f"...save {len(users_followers)} followers")
        self.mongo_client[Database.data_lake][Collection.user_followers].insert_many(users_followers)
        # print(f"...get {username} tweets")
        # user_tweets = self.twitter_client.get_all_tweets(self._user_tweets_query(), start_time)
        # print(f"...save {len(user_tweets)} {username} tweets")
        # self.mongo_client[Database.data_lake][Collection.user_tweets].insert_many(user_tweets)
        # print(f"...get search about {username}")
        # search_tweets = self.twitter_client.get_all_tweets(self._search_tweets_query(), start_time)
        # print(f"...save {len(search_tweets)} search about {username}")
        # self.mongo_client[Database.data_lake][Collection.search_tweets].insert_many(search_tweets)
        # print(f"...get {username} followers")
        # users_followers = self.twitter_service.get_users_followers(
        #     user_id=user.id,
        #     max_results_limit=user.public_metrics['followers_count'])
        # print(f"...save {len(users_followers)} followers")
        # self.mongo_client[Database.data_lake][Collection.user_followers].insert_many(users_followers)
        # print("...save fetch infos")
        # self.mongo_client[Database.config][Collection.fetch_infos].insert_one({
        #     "fetchDate": user_tweets[0]['created_at'],
        #     "tweet": {
        #         "usersFollowers": len(users_followers),
        #         "userTweets": len(user_tweets),
        #         "searchTweets": len(search_tweets)
        #     }
        # })

    def _light_dump(self, last_fetch_info):
        pass
    #     fetch_date = last_fetch_info['fetchDate']
        # start_time = datetime.strptime(fetch_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        # start_time = start_time + timedelta(seconds=2)
        # print('...dump france inter tweets')
        # user_tweets = self.twitter_service.get_all_tweets(self.user_tweets_query, start_time)
        # print('...dump search tweets')
        # search_tweets = self.twitter_service.get_all_tweets(self.search_tweets_query, start_time)
        # if user_tweets is not None and user_tweets:
        #     pass
