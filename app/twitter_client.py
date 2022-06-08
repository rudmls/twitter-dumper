import app.const as const
import requests
import os
import json
import time


def _connect_to_endpoint(url, headers, params, next_token=None):
    if next_token:
        params['next_token'] = next_token
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


class TwitterClient:
    base_url = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    def _create_headers(self):
        headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
        return headers

    def get_users_followers(self, user_id, users_number=3000, output_file="users_followers.json"):
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
            headers = self._create_headers()
            json_response = _connect_to_endpoint(search_url, headers, query_params, next_token)
            if json_response['meta']['result_count'] == 0:
                break
            for tweet in json_response['data']:
                users_stored.append(tweet)

    def get_tweets(self, query, start_time, num_tweets, output_fh):
        next_token = None
        tweets_stored = 0
        search_url = ""
        query_params = {
            'query': query,
            'start_time': start_time,
            'tweet.fields': ",".join(const.tweet_fields),
            'user.fields': ",".join(const.user_fields),
            'expansions': 'author_id',
            'max_results': 500
        }
        while tweets_stored < num_tweets:
            headers = self._create_headers()
            json_response = _connect_to_endpoint(search_url, headers, query_params, next_token)

    def get_user(self, username):
        search_url = f"{self.base_url}/users/by/username/{username}"
        query_params = {'user.fields': ",".join(const.user_fields)}
        headers = self._create_headers()
        json_response = _connect_to_endpoint(search_url, headers, query_params)
        return json_response

