import json
import logging
import os
import requests


class TweetsInterface:
    base_tweets_url = "https://api.twitter.com/2/tweets"
    endpoint_s_search_rules = "/search/stream/rules"
    endpoint_s_search = "/search/stream?tweet.fields=created_at"

    @staticmethod
    def get_rules():
        response = requests.get(
            f"{TweetsInterface.base_tweets_url}{TweetsInterface.endpoint_s_search_rules}", auth=bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        logging.info(json.dumps(response.json()))
        return response.json()

    @staticmethod
    def delete_all_rules(rules):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            f"{TweetsInterface.base_tweets_url}{TweetsInterface.endpoint_s_search_rules}",
            auth=bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        logging.info(json.dumps(response.json()))

    @staticmethod
    def setup_rules(rules):
        # cleaning up existing rules if any
        existing_rules = TweetsInterface.get_rules()
        TweetsInterface.delete_all_rules(existing_rules)

        payload = {"add": rules}
        response = requests.post(
            f"{TweetsInterface.base_tweets_url}{TweetsInterface.endpoint_s_search_rules}",
            auth=bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        logging.info(json.dumps(response.json()))

    @staticmethod
    def search_stream():
        response = requests.get(
            f"{TweetsInterface.base_tweets_url}{TweetsInterface.endpoint_s_search}", auth=bearer_oauth, stream=True, timeout=25.0,
        )

        if response.status_code == 429:
            logging.info("We've reached the limit of connections, not starting a new one...")
        elif response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )

        return response


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    bearer_token = os.environ.get("BEARER_TOKEN")

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r
