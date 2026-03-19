FROM confluentinc/cp-kafka-connect:7.5.4

# Works on 7.5.x
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:2.0.1

ENV CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components
EXPOSE 8083
CMD ["bash","-lc","connect-distributed /etc/kafka/connect-distributed.properties"]
