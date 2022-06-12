import time
from datetime import datetime, timezone, timedelta
import twitter_api.const as const
import requests

from twitter_api.exception import TooManyRequests


class BaseClient:
    _base_url = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    def _bearer_oauth(self, request):
        request.headers["Authorization"] = f"Bearer {self.bearer_token}"
        return request

    def _connect_to_endpoint(self, method, url, params=None, json=None, stream=False):
        response = requests.request(
            method=method,
            url=url,
            auth=self._bearer_oauth,
            params=params,
            json=json,
            stream=stream
        )
        if not str(response.status_code).startswith("2"):
            if response.status_code == 429:
                raise TooManyRequests(response.status_code, response.text)
            raise Exception(response.status_code, response.text)
        return response


class Client(BaseClient):

    def get_users_followers(self, user_id, users_number):
        next_token = None
        search_url = f"{self._base_url}/users/{user_id}/followers"
        users_stored = []
        query_params = {
            'tweet.fields': ",".join(const.tweet_fields),
            'user.fields': ",".join(const.user_fields),
            'expansions': 'pinned_tweet_id',
            'max_results': 1000
        }
        while len(users_stored) < users_number:
            try:
                if next_token:
                    query_params['pagination_token'] = next_token
                json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
                if json_response['meta']['result_count'] == 0:
                    break
                for user in json_response['data']:
                    users_stored.append(user)
                print(f"...{len(users_stored)} users ingested")
                try:
                    next_token = json_response["meta"]["next_token"]
                except KeyError:
                    break
            except Exception as ex:
                print(ex)
        return users_stored

    def get_all_tweets(self, query, tweets_number, start_time, end_time=None) -> tuple:
        next_token = None
        if not end_time:
            end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        tweets_stored = []
        includes_tweets_stored = []
        includes_users_stored = []
        search_url = f"{self._base_url}/tweets/search/all"
        try:
            while start_time < end_time and len(tweets_stored) < tweets_number:
                try:
                    query_params = {
                        'query': query,
                        'start_time': (start_time + timedelta(seconds=1)).strftime(const.iso_time_format),
                        'end_time': (end_time - timedelta(seconds=15)).strftime(const.iso_time_format),
                        'tweet.fields': ",".join(const.tweet_fields),
                        'user.fields': ",".join(const.user_fields),
                        'place.fields': ",".join(const.place_fields),
                        'media.fields': ",".join(const.media_fields),
                        'expansions': ",".join(const.expansions),
                        'max_results': 500
                    }
                    if next_token:
                        query_params['pagination_token'] = next_token
                    json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
                    if json_response['meta']['result_count'] == 0:
                        print("... no data")
                        break
                    # with open('json_data.json', 'w', encoding="utf-8") as outfile:
                    #     json.dump(json_response, outfile, indent=4, ensure_ascii=False)
                    # tweets
                    tweets_stored.extend(
                        list(dict(("_id", v) if k == "id" else (k, v) for k, v in _.items())
                             for _ in json_response['data']))
                    # includes tweets
                    if "tweets" in json_response["includes"]:
                        includes_tweets_stored.extend(
                            list(dict(("_id", v) if k == "id" else (k, v) for k, v in _.items())
                                 for _ in json_response["includes"]["tweets"]))
                    # includes users
                    if "users" in json_response["includes"]:
                        includes_users_stored.extend(
                            list(dict(("_id", v) if k == "id" else (k, v) for k, v in _.items())
                                 for _ in json_response["includes"]["users"]))
                    iso_time = tweets_stored[len(tweets_stored) - 1]["created_at"]
                    end_time = datetime.strptime(iso_time, const.iso_time_format)
                    print("... tweets : {} \t includes tweets : {} \t includes users : {}"
                          .format(len(tweets_stored), len(includes_tweets_stored), len(includes_users_stored)))
                    try:
                        next_token = json_response["meta"]["next_token"]
                    except KeyError:
                        break
                    time.sleep(1)
                except TooManyRequests as ex:
                    print(ex)
                    time.sleep(1)
            # remove duplicates values
            tweets_stored = {i['_id']: i for i in reversed(tweets_stored)}.values()
            includes_tweets_stored = {i['_id']: i for i in reversed(includes_tweets_stored)}.values()
            includes_users_stored = {i['_id']: i for i in reversed(includes_users_stored)}.values()
            return tweets_stored, includes_tweets_stored, includes_users_stored
        except Exception as ex:
            print(ex)

    def get_user(self, username):
        try:
            search_url = f"{self._base_url}/users/by/username/{username}"
            query_params = {'user.fields': ",".join(const.user_fields)}
            json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
            return json_response["data"]
        except Exception as ex:
            print(ex)
