-- Hive Table Definitions for Twitter Data Pipeline
-- These tables are created automatically when HiveServer2 starts.

-- 1. Raw Data (Source for Batch)
CREATE EXTERNAL TABLE IF NOT EXISTS tweets_raw_csv (
    tweet_id STRING,
    airline_sentiment STRING,
    airline_sentiment_confidence DOUBLE,
    negativereason STRING,
    negativereason_confidence DOUBLE,
    airline STRING,
    airline_sentiment_gold STRING,
    name STRING,
    negativereason_gold STRING,
    retweet_count INT,
    text STRING,
    tweet_coord STRING,
    tweet_created STRING,
    tweet_location STRING,
    user_timezone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/project/raw/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. Streamed Data (Written by Flink)
CREATE EXTERNAL TABLE IF NOT EXISTS tweets_stream_avro (
    tweet_id STRING,
    airline_sentiment STRING,
    airline STRING,
    retweet_count INT,
    text STRING,
    tweet_created STRING
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION '/project/streamed_tweets_avro/';

-- 3. Batch Results (Written by Spark)
CREATE EXTERNAL TABLE IF NOT EXISTS batch_airline_sentiment (
    airline STRING,
    total_tweets BIGINT,
    positive_count BIGINT,
    negative_count BIGINT,
    neutral_count BIGINT,
    negative_ratio DOUBLE
)
STORED AS PARQUET
LOCATION '/project/batch_results_parquet/';
