from __future__ import absolute_import

import apache_beam as beam
import apache_beam.transforms.window as window
import argparse
import json
import logging
import os

from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from datetime import datetime
from google.api_core.exceptions import InvalidArgument
from google.cloud import language_v1


def get_nl_sentiment(input_json):
    client = language_v1.LanguageServiceClient()

    # preparing document
    document = language_v1.Document(
        content=input_json['text'], type_=language_v1.Document.Type.PLAIN_TEXT
    )

    try:
        # Detects the sentiment of the text
        response = client.analyze_sentiment(
            request={"document": document}
        )
    except InvalidArgument as e:  # most likely an unsupported language was passed to the API
        logging.info(f"We got an error while calling Sentiment Analysis API: \n{e}")
        return

    result = input_json
    result['sentiment_score'] = response.document_sentiment.score
    result['sentiment_magnitude'] = response.document_sentiment.magnitude
    result['language'] = response.language

    logging.info("Final Tweet :) : {}".format(result))

    return result


def parse_response_json(input_json):
    logging.info(f"OK this is what I got {input_json}")

    json_obj = json.loads(input_json)  # parsing bytes to get a json object

    matching_rules = {'matching_rules': json_obj['matching_rules']}
    payload_json = {**json_obj['data'], **matching_rules}

    logging.info(f"Returning this {payload_json}")

    return payload_json


def run(argv=None):
    default_gcp_project = "centi-data-engineering"

    # BigQuery table schema
    tweets_bq_schema = {
        'fields': [
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "text", "type": "STRING", "mode": "NULLABLE"},
            {"name": "matching_rules", "type": "RECORD", "mode": "REPEATED",
             "fields": [
                 {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
                 {"name": "tag", "type": "STRING", "mode": "NULLABLE"},
             ]
             },
            {"name": "sentiment_score", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sentiment_magnitude", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "language", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    }

    # Building our beam pipeline
    parser = argparse.ArgumentParser(prog='Tweets Streamer')
    parser.add_argument('--pubsub_subscription', help='Input PubSub subscription name', default="tweet_stream-sub")
    parser.add_argument('--pubsub_project', help='Project Id of the subscription', required=True)
    custom_args, pipeline_args = parser.parse_known_args(argv)

    subscription_fullname = f"projects/{custom_args.pubsub_project}/subscriptions/{custom_args.pubsub_subscription}"

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    # Run on Cloud Dataflow by default
    # pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    options_dict = pipeline_options.get_all_options()
    gcp_project = options_dict['project'] if options_dict['project'] is not None else default_gcp_project

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = gcp_project
    google_cloud_options.region = 'us-central1'
    google_cloud_options.temp_location = 'gs://dataflow-runs-centi/temp'
    google_cloud_options.staging_location = 'gs://dataflow-runs-centi/staging'


    p = beam.Pipeline(options=pipeline_options)

    # reading pubsub with streamed tweets
    tweets_stream = p | "Reading tweets" >> beam.io.ReadFromPubSub(
        subscription=subscription_fullname,
        with_attributes=False,
        # id_label="id"
    )

    # formatting the tweets
    parsed_tweets = (tweets_stream
                     | 'Formatting json' >> beam.Map(parse_response_json)
                     | 'Obtain sentiment' >> beam.Map(get_nl_sentiment)
                     )

    # sending data to BigQuery
    parsed_tweets | 'Saving tweets' >> beam.io.WriteToBigQuery(
        table="streamed_tweets",
        dataset="rs_analytics",
        schema=tweets_bq_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project=gcp_project,
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
