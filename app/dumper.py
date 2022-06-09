import json
from dataclasses import dataclass
from pymongo import MongoClient

import twitter_api
from app.config import Config


class Database:
    stream_data: str = "stream_data"
    data_lake: str = "data_lake"
    config: str = "config"


class Collection:
    user_followers: str = "users_followers"
    user_tweets: str = "user_tweets"
    search_tweets: str = "search_tweets"
    fetch_infos: str = "fetch_infos"
    stream_search_tweets: str = "stream_search_tweets"


@dataclass
class Dumper:
    _mongo_client: MongoClient
    _config: Config
    _twitter_client: twitter_api.Client
    _twitter_streaming_client: twitter_api.StreamingClient

    def __init__(self, config: Config):
        self._twitter_client = twitter_api.Client(config.twitter.bearer_token)
        self._twitter_streaming_client = twitter_api.StreamingClient(config.twitter.bearer_token)
        self._config = config
        self._mongo_client = MongoClient(
            f'mongodb+srv://{config.mongo_db.username}:{config.mongo_db.password}@{config.mongo_db.host}')

    def _user_tweets_query(self):
        username = self._config.dumper.username
        return f"from:{username} -is:retweet"

    def _search_tweets_query(self):
        username = self._config.dumper.username
        return f"@{username} -from:{username} -is:retweet"

    def start_streaming(self):
        rules = [{
            "value": self._search_tweets_query(),
            "tag": self._config.dumper.username
        }]
        stored_rules = self._twitter_streaming_client.get_rules()
        delete = self._twitter_streaming_client.delete_all_rules(stored_rules)
        print(delete)
        create = self._twitter_streaming_client.set_rules(rules)
        print(create)
        response = self._twitter_streaming_client.get_stream()
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                self._mongo_client[Database.stream_data][Collection.stream_search_tweets].insert_one(json_response)
                print(json.dumps(json_response, indent=4, ensure_ascii=False))

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
        username = self._config.dumper.username
        start_time = self._config.dumper.tweet.start_time
        # print(f"...get {username} user info")
        # user = self.twitter_client.get_user(username=username)
        # print(f"...get {username} followers")
        # users_followers = self.twitter_client.get_users_followers(user["id"], 3000)
        # print(f"...save {len(users_followers)} followers")
        # self.mongo_client[Database.data_lake][Collection.user_followers].insert_many(users_followers)
        print(f"...get {username} tweets")
        user_tweets = self._twitter_client.get_all_tweets(self._user_tweets_query(), start_time, 1000)
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
