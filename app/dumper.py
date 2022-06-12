import json
import pymongo
import twitter_api
from dataclasses import dataclass
from app.config import Config
from app.mongodb_cluster import MongoDbCluster


@dataclass
class Dumper:
    _mongodb_cluster: MongoDbCluster
    _config: Config
    _twitter_client: twitter_api.Client
    _twitter_streaming_client: twitter_api.StreamingClient

    def __init__(self, config: Config):
        self._twitter_client = twitter_api.Client(config.twitter.bearer_token)
        self._twitter_streaming_client = twitter_api.StreamingClient(config.twitter.bearer_token)
        self._config = config
        self._mongodb_cluster = MongoDbCluster(config.mongo_db)

    def start(self):
        try:
            print("[ dump user tweets ]")
            self._dump_user_tweets()
            print("[ dump search tweets ]")
            self._dump_search_tweets()
        except Exception as ex:
            print(ex)

    def start_streaming(self):
        rules = [{"value": self._search_tweets_query(), "tag": self._config.dumper.username}]
        stored_rules = self._twitter_streaming_client.get_rules()
        delete = self._twitter_streaming_client.delete_all_rules(stored_rules)
        print(delete)
        create = self._twitter_streaming_client.set_rules(rules)
        print(create)
        response = self._twitter_streaming_client.get_stream()
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                # self._mongo_client[Database.stream_data][Collection.stream_search_tweets].insert_one(json_response)
                print(json.dumps(json_response, indent=4, ensure_ascii=False))

    def _user_tweets_query(self):
        username = self._config.dumper.username
        return f"from:{username} -is:retweet"

    def _search_tweets_query(self):
        username = self._config.dumper.username
        return f"@{username} -from:{username} -is:retweet"

    def _dump_users_followers(self):
        username = self._config.dumper.username
        user = self._twitter_client.get_user(username=username)
        users_followers = self._twitter_client.get_users_followers(user["id"], 15000)
        self._mongodb_cluster.save_users_followers(users_followers)

    def _dump_user_tweets(self):
        start_time = self._mongodb_cluster.get_tweet_created_at("user_tweets", pymongo.DESCENDING)
        user_tweets, includes_tweets, includes_users = \
            self._twitter_client.get_all_tweets(self._user_tweets_query(), 15000, start_time)
        self._mongodb_cluster.save_user_tweets(user_tweets)
        self._mongodb_cluster.save_include_tweets(includes_tweets)
        self._mongodb_cluster.save_includes_users(includes_users)

    def _dump_search_tweets(self):
        start_time = self._mongodb_cluster.get_tweet_created_at("search_tweets", pymongo.DESCENDING)
        search_tweets, includes_tweets, includes_users = \
            self._twitter_client.get_all_tweets(self._search_tweets_query(), 15000, start_time)
        self._mongodb_cluster.save_search_tweets(search_tweets)
        self._mongodb_cluster.save_include_tweets(includes_tweets)
        self._mongodb_cluster.save_includes_users(includes_users)
