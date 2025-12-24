#!/bin/bash
set -e

# 1. Start HiveServer2 in the background
echo "--> Starting HiveServer2..."
/opt/hive/bin/hive --service hiveserver2 --hiveconf hive.metastore.uris=thrift://hive-metastore:9083 &
HS2_PID=$!

# 2. Wait for it to become ready
echo "--> Waiting for HiveServer2 to be ready..."
until beeline -u "jdbc:hive2://localhost:10000/default" -n hive -e "SHOW DATABASES;" >/dev/null 2>&1; do
    echo "Waiting for HiveJDBC..."
    sleep 2
done
echo "--> HiveServer2 is UP!"

# 3. Initialize Tables from SQL file
echo "--> Running Initialization SQL..."
beeline -u "jdbc:hive2://localhost:10000/default" -n hive -f /sql/hive_tables.sql
echo "--> HiveInitialization Complete."

# 4. Wait for the background process to exit (keep container alive)
wait $HS2_PID
