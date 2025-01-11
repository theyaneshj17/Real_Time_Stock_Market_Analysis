from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'stockprice',  # The topic to subscribe to
    bootstrap_servers='localhost:9092',  # Kafka brokers
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume and print messages
for message in consumer:
    print(message.value)  # Print the message value
