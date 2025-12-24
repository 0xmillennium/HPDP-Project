FROM apache/spark:latest

USER root

# Install curl
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Directory for jars
WORKDIR /opt/spark/jars

# --- Download Postgres Driver ---
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o postgresql-42.6.0.jar

# --- Download Hive 4.0.0 Client Jars ---
# Maven Central Base URL
ARG MAVEN_REPO=https://repo1.maven.org/maven2

# Core Hive Artifacts
RUN curl -L ${MAVEN_REPO}/org/apache/hive/hive-metastore/4.0.0/hive-metastore-4.0.0.jar -o hive-metastore-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/hive-exec/4.0.0/hive-exec-4.0.0-core.jar -o hive-exec-4.0.0-core.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/hive-common/4.0.0/hive-common-4.0.0.jar -o hive-common-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/hive-serde/4.0.0/hive-serde-4.0.0.jar -o hive-serde-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/hive-service-rpc/4.0.0/hive-service-rpc-4.0.0.jar -o hive-service-rpc-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/hive-kunszt/4.0.0/hive-kunszt-4.0.0.jar -o hive-kunszt-4.0.0.jar || true

# Dependencies (Thrift 0.16.0 is required for Hive 4.0.0)
RUN curl -L ${MAVEN_REPO}/org/apache/thrift/libthrift/0.16.0/libthrift-0.16.0.jar -o libthrift-0.16.0.jar

# Hive Shims (Required to interface with Hadoop)
RUN curl -L ${MAVEN_REPO}/org/apache/hive/hive-shims/4.0.0/hive-shims-4.0.0.jar -o hive-shims-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/shims/hive-shims-common/4.0.0/hive-shims-common-4.0.0.jar -o hive-shims-common-4.0.0.jar && \
    curl -L ${MAVEN_REPO}/org/apache/hive/shims/hive-shims-0.23/4.0.0/hive-shims-0.23-4.0.0.jar -o hive-shims-0.23-4.0.0.jar

# Fix permissions
RUN chmod 644 /opt/spark/jars/*.jar

USER spark
WORKDIR /opt/spark/work-dir