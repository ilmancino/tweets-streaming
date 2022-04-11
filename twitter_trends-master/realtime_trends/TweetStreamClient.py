import threading
import logging
import time
from PubSubInterface import *
from TweetsInterface import *

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "centi-data-engineering")
PUBSUB_TOPIC_NAME = 'tweet_stream'


def process_stream():
    response = TweetsInterface.search_stream()

    logging.info("Creating pubsub client...")
    pubsub_client = PubSubInterface(project_id=GCP_PROJECT_ID, topic_name=PUBSUB_TOPIC_NAME)
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)

            logging.info(json.dumps(json_response, indent=4, sort_keys=True))

            if 'errors' not in json_response:
                pubsub_client.publish_tweet(json.dumps(json_response))
            else:
                # sometimes status code from Twitter is 200 but payload contains errors like Operational Errors response
                logging.info("Errors received from Twitter API. Not publishing...")


def listen_for_tweets(rules, listener_name=None):

    # setting up rules to filter tweets
    TweetsInterface.setup_rules(rules)

    conn = False
    # loop to keep our connection alive
    while 1 > 0:
        # check if we have no connection
        logging.info("Checking if we don't have a connection to start one...")
        if not conn:
            # initiates tweets stream listener in a separate thread
            try:
                conn = True
                t = threading.Thread(target=process_stream, name=listener_name)
                t.run()
            except requests.exceptions.ConnectionError as e:
                logging.info(f"We got a TimeOut error: \n{e}")
                conn = False

        if not t.is_alive():
            logging.info("Thread is not Alive!")
            conn = False
        else:
            logging.info("Still Alive!")

        logging.info("Waiting 10 seconds before requesting a new connection...")
        time.sleep(10)


def main():
    rules = [
        {"value": "-is:retweet CONCACAF", "tag": "CONCACAF"},
        {"value": "-is:retweet CONMEBOL", "tag": "CONMEBOL"},
        {"value": "-is:retweet UEFA", "tag": "UEFA"},
    ]

    listen_for_tweets(rules, listener_name="TwitterClient")


if __name__ == "__main__":
    main()
