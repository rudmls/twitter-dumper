import time
from datetime import datetime, timezone, timedelta
import twitter_api.const as const
import requests


class BaseClient:
    _base_url_v1 = "https://api.twitter.com/1.1"
    _base_url_v2 = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str):
        self.bearer_token = bearer_token

    def _bearer_oauth(self, request):
        request.headers["Authorization"] = f"Bearer {self.bearer_token}"
        return request

    def _connect_to_endpoint(self, method, url, params=None, json=None, stream=False):
        response = requests.request(
            method=method, url=url, auth=self._bearer_oauth,
            params=params, json=json, stream=stream)
        if response.status_code != 200:
            # Too many requests error
            if response.status_code == 429:
                rate_limit_reset = datetime.fromtimestamp(int(response.headers["x-rate-limit-reset"]))
                time_to_wait = (rate_limit_reset - datetime.now()).total_seconds()
                print("To many request. \t Time to wait : {:0.2f} s \t Rate limit reset : {}"
                      .format(time_to_wait, rate_limit_reset))
                time.sleep(time_to_wait)
            # Twitter internal server error
            elif response.status_code == 500:
                print("Internal server error. Time to wait : {:0.2f}".format(30))
                time.sleep(30)
            # Twitter service unavailable error
            elif response.status_code == 503:
                print("Service unavailable error. Time to wait : {:0.2f}".format(30))
                time.sleep(30)
            else:
                raise Exception("Request returned an error: {} {}"
                                .format(response.status_code, response.text))
        elif response.ok:
            return response


class Client(BaseClient):

    def get_subscribed_users(self, screen_name: str):
        users_stored = []
        cursor = None
        search_url = f"{self._base_url_v1}/followers/ids.json"
        query_params = {"screen_name": screen_name, "count": 5000}
        try:
            while True:
                if cursor:
                    query_params['cursor'] = cursor
                start = time.time()
                json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
                cursor = json_response["next_cursor"]
                if cursor == 0:
                    break
                users_ids = json_response["ids"]
                current_users_stored = self.get_users_by_ids(users_ids)
                users_stored.extend(current_users_stored)
                duration = time.time() - start
                print("... users stored : {} \t duration : {:0.2f} s".format(len(users_stored), duration))
                if duration < 60:
                    time_to_sleep = 60 - duration
                    print("... sleep for {:0.2f} s".format(time_to_sleep))
                    time.sleep(60 - duration)
            return users_stored
        except Exception as ex:
            print(ex)

    def get_users_by_ids(self, users_ids: list):
        users_stored = []
        search_url = f"{self._base_url_v2}/users"
        query_params = {"user.fields": ",".join(const.user_fields)}
        try:
            ids_split_in_100 = [users_ids[i:i + 100] for i in range(0, len(users_ids), 100)]
            for ids in ids_split_in_100:
                query_params['ids'] = ",".join(str(elt) for elt in ids)
                json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
                if "data" not in json_response:
                    continue
                users_stored.extend(
                    list(dict(("_id", v) if k == "id" else (k, v) for k, v in _.items())
                         for _ in json_response['data']))
            return users_stored
        except Exception as ex:
            print(ex)

    def get_all_tweets(self, query, tweets_number, start_time, end_time=None) -> tuple:
        next_token = None
        if not end_time:
            end_time = datetime.now(timezone.utc).replace(tzinfo=None)
        tweets_stored = []
        includes_tweets_stored = []
        includes_users_stored = []
        search_url = f"{self._base_url_v2}/tweets/search/all"
        try:
            while start_time < end_time and len(tweets_stored) < tweets_number:
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
            # remove duplicates values
            tweets_stored = {i['_id']: i for i in reversed(tweets_stored)}.values()
            includes_tweets_stored = {i['_id']: i for i in reversed(includes_tweets_stored)}.values()
            includes_users_stored = {i['_id']: i for i in reversed(includes_users_stored)}.values()
            return tweets_stored, includes_tweets_stored, includes_users_stored
        except Exception as ex:
            print(ex)

    def get_user(self, username):
        try:
            search_url = f"{self._base_url_v2}/users/by/username/{username}"
            query_params = {'user.fields': ",".join(const.user_fields)}
            json_response = self._connect_to_endpoint("GET", search_url, query_params).json()
            return json_response["data"]
        except Exception as ex:
            print(ex)
