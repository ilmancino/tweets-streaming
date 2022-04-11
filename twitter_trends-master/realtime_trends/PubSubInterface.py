import logging
from google.cloud import pubsub_v1


class PubSubInterface:

    def __init__(self, project_id=None, topic_name=None):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic = 'projects/{project_id}/topics/{topic}'.format(
            project_id=project_id,
            topic=topic_name,
            )

    def set_topic(self, project_id=None, topic_name=None):
        self.topic = 'projects/{project_id}/topics/{topic}'.format(
            project_id=project_id,
            topic=topic_name,
        )

    def publish_tweet(self, data):
        logging.info("I will publish data now ;) ...")
        self.publisher.publish(topic=self.topic, data=data.encode('utf-8'))
