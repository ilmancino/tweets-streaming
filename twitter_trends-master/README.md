# twitter_trends

How to run the pipeline in Dataflow?
python3 realtime_trends/TweetsProcessor.py --pubsub_project centi-data-engineering --runner DataflowRunner --autoscaling_algorithm=NONE --num_workers 1 --requirements_file requirements_df.txt
