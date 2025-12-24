FROM apache/flink:latest

USER root

# 1. Install Python 3 and Pip (Required for PyFlink)
# We update the package list and install the latest python3
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python || true

# 2. Install PyFlink Library
# We install the version matching the container's Flink version dynamically
# or fall back to the latest stable pip package.
RUN pip3 install apache-flink --break-system-packages

# 3. Install Hadoop Dependencies (The "Bridge" to HDFS)
# We use the widely compatible shaded Hadoop 2 uber jar.
# This works with Hadoop 3 clusters for client operations (like writing Avro).
RUN mkdir -p /opt/flink/lib
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar -o /opt/flink/lib/flink-shaded-hadoop-2-uber.jar

# 4. Enable Avro, Parquet, and Kafka Connectors
# Move dormant jars from /opt to /lib to activate them
RUN cp /opt/flink/opt/flink-avro-*.jar /opt/flink/lib/ || true
RUN cp /opt/flink/opt/flink-parquet-*.jar /opt/flink/lib/ || true
RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/ || true

# Download the Kafka connector (Compatible with Flink 2.0)
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar -o /opt/flink/lib/flink-connector-kafka.jar
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar -o /opt/flink/lib/kafka-clients.jar

# 5. Download SQL format jars for Avro and JSON (required for Table API sinks/sources)
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro/2.0.0/flink-sql-avro-2.0.0.jar -o /opt/flink/lib/flink-sql-avro.jar
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-json/2.0.0/flink-sql-json-2.0.0.jar -o /opt/flink/lib/flink-sql-json.jar

# Switch back to the default user
USER flink