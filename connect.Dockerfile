FROM confluentinc/cp-kafka-connect:7.6.1

RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.12.0

# Keep default entrypoint from the base image
