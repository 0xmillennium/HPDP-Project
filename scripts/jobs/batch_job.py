from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

def main():
    # 1. Initialize Spark Session with Hive Support
    spark = SparkSession.builder \
        .appName("TwitterBatchAnalysis") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Spark Session Created. Reading from Hive...")

    # Read from the tweets_raw_csv table (created by HiveServer2 on startup)
    df_raw = spark.sql("SELECT * FROM tweets_raw_csv")

    # 3. Perform Aggregation
    # We want: airline, total, positive, negative, neutral, negative_ratio
    result_df = df_raw.groupBy("airline").agg(
        count("*").alias("total_tweets"),
        count(when(col("airline_sentiment") == "positive", True)).alias("positive_count"),
        count(when(col("airline_sentiment") == "negative", True)).alias("negative_count"),
        count(when(col("airline_sentiment") == "neutral", True)).alias("neutral_count")
    ).withColumn(
        "negative_ratio", 
        col("negative_count") / col("total_tweets")
    )

    print("Aggregation Complete. Sample results:")
    result_df.show(5)

    # 4. Write Results back to Hive (Parquet)
    # This inserts data into the table defined in our init script
    print("Writing results to Hive table 'batch_airline_sentiment'...")
    result_df.write \
        .mode("overwrite") \
        .insertInto("batch_airline_sentiment")

    print("Batch Job Successfully Completed.")
    spark.stop()

if __name__ == "__main__":
    main()