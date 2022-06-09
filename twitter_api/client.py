from datetime import datetime, timezone, timedelta

import app.const as const
import requests
import time


class BaseClient:
    base_url = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    def _bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r

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
            raise Exception(response.status_code, response.text)
        return response

    def _create_headers(self):
        return {"Authorization": "Bearer {}".format(self.bearer_token)}


class Client(BaseClient):

    def get_users_followers(self, user_id, users_number):
        next_token = None
        search_url = f"{self.base_url}/users/{user_id}/followers"
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

    def get_all_tweets(self, query, start_time, tweets_number):
        next_token = None
        end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        tweets_stored = []
        search_url = f"{self.base_url}/tweets/search/all"
        while start_time < end_time and len(tweets_stored) < tweets_number:
            try:
                query_params = {
                    'query': query,
                    'start_time': start_time.strftime(const.iso_time_format),
                    'end_time': (end_time - timedelta(seconds=15)).strftime(const.iso_time_format),
                    'tweet.fields': ",".join(const.tweet_fields),
                    'user.fields': ",".join(const.user_fields),
                    'expansions': 'author_id',
                    'max_results': 500
                }
                if next_token:
                    query_params['pagination_token'] = next_token
                json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
                if json_response['meta']['result_count'] == 0:
                    break
                for tweet in json_response['data']:
                    tweets_stored.append(tweet)
                print(f"...{len(tweets_stored)} tweets ingested")
                iso_time = tweets_stored[len(tweets_stored) - 1]["created_at"]
                end_time = datetime.strptime(iso_time, const.iso_time_format)
                try:
                    next_token = json_response["meta"]["next_token"]
                except KeyError:
                    break
                time.sleep(1)
            except Exception as ex:
                print(ex)
        return tweets_stored

    def get_user(self, username):
        try:
            search_url = f"{self.base_url}/users/by/username/{username}"
            query_params = {'user.fields': ",".join(const.user_fields)}
            json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
            return json_response["data"]
        except Exception as ex:
            print(ex)
