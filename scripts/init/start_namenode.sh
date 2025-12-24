#!/bin/bash
set -e

# 1. Format and Start NameNode in the background
echo "--> Starting HDFS NameNode..."
hdfs namenode -format -force -nonInteractive || true
hdfs namenode &
NN_PID=$!

# 2. Wait for NameNode to leave safe mode
echo "--> Waiting for NameNode to be ready..."
until hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'OFF'; do
    echo "Waiting for safe mode OFF..."
    sleep 2
done
echo "--> NameNode is UP!"

# 3. Wait for at least one DataNode to register
echo "--> Waiting for DataNode..."
until hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; do
    echo "Waiting for DataNode..."
    sleep 2
done
echo "--> DataNode registered!"

# 4. Setup HDFS Directories
echo "--> Setting up HDFS directories..."
hdfs dfs -mkdir -p /project/raw
hdfs dfs -mkdir -p /project/streamed_tweets_avro
hdfs dfs -mkdir -p /project/batch_results_parquet
hdfs dfs -chmod -R 777 /project

# 5. Upload Data
echo "--> Checking dataset..."
if hdfs dfs -test -e /project/raw/Tweets.csv; then
    echo "Tweets.csv already exists in HDFS. Skipping upload."
else
    if [ -f "/local_data/Tweets.csv" ]; then
        echo "Uploading Tweets.csv..."
        hdfs dfs -put /local_data/Tweets.csv /project/raw/
        echo "Upload complete."
    else
        echo "WARNING: /local_data/Tweets.csv not found."
    fi
fi

echo "--> HDFS Initialization Complete."

# 6. Wait for the background process (keep container alive)
wait $NN_PID