FROM confluentinc/cp-kafka-connect:7.6.1

# Preinstall MongoDB Kafka Connector to avoid runtime downloads
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.12.0

# Keep default entrypoint from the base image
