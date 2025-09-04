How to Run

Start Kafka + Kafka Connect

docker compose up -d kafka kafka-connect


Wait for Kafka Connect & register Mongo sink

docker compose exec kafka-connect sh -lc "/workspace/connect/start-and-wait.sh"


This creates the connector mongo-sink pointing to your MongoDB.

Build and start Producer + Consumer

docker compose --profile apps build producer consumer
docker compose --profile apps up -d producer consumer


View logs

docker compose logs -f producer
docker compose logs -f consumer


Producer will show weather data being published to Kafka.

Consumer will print messages received from Kafka.

MongoDB Atlas will contain documents in database weather, collection events.