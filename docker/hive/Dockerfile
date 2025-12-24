FROM apache/hive:4.0.0

USER root

# Install curl
RUN apt-get update && apt-get install -y curl

# Download Postgres JDBC Driver (Required for Metastore)
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o /opt/hive/lib/postgresql-42.6.0.jar

USER hive