import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    # 1. Setup Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Use Table API for SQL-like simplicity
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Define Kafka Source Table (JSON)
    # We define a temporary view to read from Kafka
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE source_kafka (
            tweet_id STRING,
            airline_sentiment STRING,
            airline STRING,
            retweet_count STRING,
            text STRING,
            tweet_created STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets_topic',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # 3. Define HDFS Sink Table (Avro) with File-Rolling Policies
    # We point to the location where Hive expects the data
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE sink_hdfs (
            tweet_id STRING,
            airline_sentiment STRING,
            airline STRING,
            retweet_count INT,
            text STRING,
            tweet_created STRING,
            dt STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://namenode:9000/project/streamed_tweets_avro',
            'format' = 'avro',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.partition-commit.delay' = '1 min',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '10 min',
            'sink.rolling-policy.check-interval' = '1 min'
        )
    """)

    # 4. Define Console Sink (For Alerts)
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE sink_console (
            alert_message STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # 5. Core Logic - Both sinks execute in parallel (per project requirement)
    stmt_set = t_env.create_statement_set()

    # Sink 1: Write raw data to HDFS (Avro) with partition
    stmt_set.add_insert_sql("""
        INSERT INTO sink_hdfs
        SELECT 
            tweet_id, 
            airline_sentiment, 
            airline, 
            CAST(retweet_count AS INT), 
            text, 
            tweet_created,
            SUBSTRING(tweet_created, 1, 10) as dt
        FROM source_kafka
    """)

    # Sink 2: Use Case A - Alert on negative tweets
    stmt_set.add_insert_sql("""
        INSERT INTO sink_console
        SELECT CONCAT('ALERT [', airline, ']: ', text)
        FROM source_kafka
        WHERE airline_sentiment = 'negative'
    """)

    # Execute both sinks in parallel
    stmt_set.execute().wait()

if __name__ == '__main__':
    main()